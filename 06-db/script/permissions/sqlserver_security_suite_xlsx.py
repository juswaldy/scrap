#!/usr/bin/env python3
"""
sqlserver_security_suite_xlsx.py

From-scratch, **XLSX-driven** rebuild of SQL Server security at both DATABASE and SERVER scopes:
  - Reads ONE Excel workbook with 8 sheets (tabs):
      1) Roles
      2) Users
      3) Permissions
      4) Memberships
      5) ServerPermissions
      6) ServerRoleMemberships
      7) Logins
      8) Endpoints
  - Database: roles, users, GRANT/DENY permissions, role memberships
  - Server: logins (provision), endpoints (create/manage), GRANT/DENY permissions
            on SERVER, ENDPOINT, and AVAILABILITY GROUP securables, server role memberships
  - Cleanup: drops ALL DB role memberships and revokes ALL explicit DB permissions;
             at server-level, revokes all explicit permissions and removes all server role
             memberships for the discovered "managed login" set.
  - Reporting: detects potential GRANTs that will be overridden by DENYs (CSV-level heuristic)
               and emits diagnostic T-SQL to list conflicts that exist after applying changes.

USAGE
-----
python sqlserver_security_suite_xlsx.py \
  --xlsx /path/to/security_input.xlsx \
  --out-sql security_rebuild.sql \
  [--execute --conn-str "Driver={ODBC Driver 18 for SQL Server};Server=.;Database=MyDb;Trusted_Connection=Yes;TrustServerCertificate=Yes;"] \
  [--db-hint MyDb]

REQUIRES: Python 3.9+. Libraries: pandas, openpyxl. Optional: pyodbc for --execute.
TARGET: SQL Server 2017+ (uses STRING_AGG). See notes to adapt for older versions.
"""

import sys
import argparse
from typing import List, Dict, Tuple, Set, Optional

import pandas as pd

debugging = False

# -----------------------
# Allow-lists & constants
# -----------------------

DB_ALLOWED_PERMS = {
    "DATABASE": {
        "CONNECT", "SELECT", "INSERT", "UPDATE", "DELETE",
        "ALTER ANY SCHEMA", "ALTER ANY USER", "CONTROL", "CONTROL DATABASE",
        "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE", "CREATE FUNCTION",
        "VIEW DATABASE STATE", "VIEW DEFINITION"
    },
    "SCHEMA": {
        "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE",
        "CONTROL", "ALTER", "REFERENCES", "VIEW DEFINITION"
    },
    "OBJECT": {
        "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE",
        "REFERENCES", "CONTROL", "VIEW DEFINITION"
    }
}

SERVER_ALLOWED_PERMS = {
    "SERVER": {
        "ALTER ANY CONNECTION", "ALTER ANY CREDENTIAL", "ALTER ANY ENDPOINT", "ALTER ANY EVENT SESSION",
        "ALTER ANY LINKED SERVER", "ALTER ANY LOGIN", "ALTER ANY SERVER AUDIT", "ALTER ANY SERVER ROLE",
        "ALTER ANY AVAILABILITY GROUP",
        "ALTER RESOURCES", "ALTER SETTINGS", "ALTER TRACE", "AUTHENTICATE SERVER", "CONTROL SERVER",
        "CONNECT SQL", "CREATE ANY DATABASE", "CREATE DDL EVENT NOTIFICATION", "EXTERNAL ACCESS ASSEMBLY",
        "IMPERSONATE ANY LOGIN", "SELECT ALL USER SECURABLES", "SHUTDOWN", "UNSAFE ASSEMBLY",
        "VIEW ANY DATABASE", "VIEW ANY DEFINITION", "VIEW SERVER STATE", "ADMINISTER BULK OPERATIONS"
    },
    "ENDPOINT": {"CONNECT", "ALTER", "CONTROL", "TAKE OWNERSHIP", "VIEW DEFINITION"},
    "AVAILABILITY_GROUP": {"ALTER", "CONTROL", "VIEW DEFINITION", "TAKE OWNERSHIP"}
}

SERVER_ROLES_ALLOWLIST = {
    "sysadmin", "securityadmin", "serveradmin", "processadmin", "setupadmin",
    "diskadmin", "bulkadmin", "dbcreator"
}

LOGIN_TYPES = {"SQL", "WINDOWS", "EXTERNAL"}  # scope of this tool

# -----------------------
# Utilities
# -----------------------

def warn(msg: str):
    print(f"WARNING: {msg}", file=sys.stderr)

def info(msg: str):
    print(f"[INFO] {msg}")

def norm(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        # Avoid .0 printing for numeric-like content; but we treat all as strings
        return str(int(v)) if v.is_integer() else str(v)
    return str(v).strip()

def as_bool(v, default=False) -> bool:
    s = norm(v).lower()
    if s == "":
        return default
    return s in {"true", "1", "y", "yes", "t"}

def bracket(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"

def nliteral(s: str) -> str:
    return "N'" + s.replace("'", "''") + "'"

def require_columns(df: pd.DataFrame, required: Set[str], sheet: str):
    cols = {c.strip() for c in df.columns}
    missing = required - cols
    if missing:
        sys.exit(f"ERROR: Sheet '{sheet}' missing required columns: {', '.join(sorted(missing))}")

# -----------------------
# Loaders & validation from DataFrames
# -----------------------

def load_roles_df(df: pd.DataFrame) -> Set[str]:
    require_columns(df, {"RoleName"}, "Roles")
    roles: Set[str] = set()
    seen: Set[str] = set()
    for i, row in df.iterrows():
        r = norm(row.get("RoleName"))
        if not r:
            warn(f"Roles row {i+2}: empty RoleName; skipped."); continue
        key = r.lower()
        if key in seen:
            warn(f"Roles row {i+2}: duplicate role '{r}'; de-duplicated."); continue
        if len(r) > 128:
            warn(f"Roles row {i+2}: '{r}' exceeds 128 chars; skipped."); continue
        seen.add(key); roles.add(r)
    return roles

def load_users_df(df: pd.DataFrame) -> List[Dict]:
    require_columns(df, {"UserName", "AuthType"}, "Users")
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    for i, row in df.iterrows():
        u = norm(row.get("UserName"))
        authtype = norm(row.get("AuthType")).upper()
        login = norm(row.get("LoginName"))
        defschema = norm(row.get("DefaultSchema")) or "dbo"
        if not u or not authtype:
            warn(f"Users row {i+2}: missing UserName/AuthType; skipped."); continue
        if authtype not in {"LOGIN", "WITHOUT_LOGIN", "EXTERNAL"}:
            warn(f"Users row {i+2}: invalid AuthType '{authtype}'; skipped."); continue
        if authtype == "LOGIN" and not login:
            warn(f"Users row {i+2}: AuthType=LOGIN requires LoginName; skipped."); continue
        key = (u.lower(), authtype, login.lower())
        if key in seen:
            warn(f"Users row {i+2}: duplicate user '{u}' with same mapping; de-duplicated."); continue
        seen.add(key)
        rows.append({"UserName": u, "AuthType": authtype, "LoginName": login if authtype == "LOGIN" else None, "DefaultSchema": defschema})
    return rows

def load_db_permissions_df(df: pd.DataFrame, valid_roles: Set[str]) -> List[Dict]:
    require_columns(df, {"RoleName", "Scope", "Permission"}, "Permissions")
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    for i, row in df.iterrows():
        role = norm(row.get("RoleName"))
        state = norm(row.get("State") or "GRANT").upper()
        scope = norm(row.get("Scope")).upper()
        perm  = norm(row.get("Permission")).upper()
        schema = norm(row.get("SchemaName"))
        obj = norm(row.get("ObjectName"))
        wgo = as_bool(row.get("WithGrantOption"), False)

        if not role or not scope or not perm:
            warn(f"Permissions row {i+2}: missing RoleName/Scope/Permission; skipped."); continue
        if role not in valid_roles:
            warn(f"Permissions row {i+2}: Role '{role}' not defined in Roles; ignored."); continue
        if state not in {"GRANT", "DENY"}:
            warn(f"Permissions row {i+2}: invalid State '{state}'; skipped."); continue
        if scope not in {"DATABASE", "SCHEMA", "OBJECT"}:
            warn(f"Permissions row {i+2}: invalid Scope '{scope}'; skipped."); continue
        if scope in {"SCHEMA", "OBJECT"} and not schema:
            warn(f"Permissions row {i+2}: SchemaName required for Scope={scope}; skipped."); continue
        if scope == "OBJECT" and not obj:
            warn(f"Permissions row {i+2}: ObjectName required for Scope=OBJECT; skipped."); continue
        if perm not in DB_ALLOWED_PERMS.get(scope, set()):
            warn(f"Permissions row {i+2}: Permission '{perm}' not allowed for Scope={scope}; skipped."); continue
        if state == "DENY" and wgo:
            warn(f"Permissions row {i+2}: WithGrantOption ignored for DENY."); wgo = False

        sig = (role.lower(), state, scope, (schema or "").lower(), (obj or "").lower(), perm, wgo)
        if sig in seen:
            warn(f"Permissions row {i+2}: duplicate DB permission row; de-duplicated."); continue
        seen.add(sig)
        rows.append({
            "RoleName": role, "State": state, "Scope": scope, "SchemaName": schema or None,
            "ObjectName": obj or None, "Permission": perm, "WithGrantOption": wgo
        })
    return rows

def load_db_memberships_df(df: pd.DataFrame, valid_roles: Set[str]) -> List[Dict]:
    require_columns(df, {"UserName", "RoleName"}, "Memberships")
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    for i, row in df.iterrows():
        user = norm(row.get("UserName"))
        role = norm(row.get("RoleName"))
        if not user or not role:
            warn(f"Memberships row {i+2}: missing UserName/RoleName; skipped."); continue
        if role not in valid_roles:
            warn(f"Memberships row {i+2}: Role '{role}' not in Roles; ignored."); continue
        sig = (user.lower(), role.lower())
        if sig in seen:
            warn(f"Memberships row {i+2}: duplicate membership; de-duplicated."); continue
        seen.add(sig); rows.append({"UserName": user, "RoleName": role})
    return rows

def load_server_permissions_df(df: pd.DataFrame) -> List[Dict]:
    require_columns(df, {"LoginName", "State", "Scope", "Permission"}, "ServerPermissions")
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    for i, row in df.iterrows():
        login = norm(row.get("LoginName"))
        state = norm(row.get("State")).upper()
        scope = norm(row.get("Scope")).upper()
        perm  = norm(row.get("Permission")).upper()
        endpoint = norm(row.get("EndpointName"))
        agname = norm(row.get("AGName"))
        wgo = as_bool(row.get("WithGrantOption"), False)

        if not login or not state or not scope or not perm:
            warn(f"ServerPermissions row {i+2}: missing LoginName/State/Scope/Permission; skipped."); continue
        if state not in {"GRANT", "DENY"}:
            warn(f"ServerPermissions row {i+2}: invalid State '{state}'; skipped."); continue
        if scope not in {"SERVER", "ENDPOINT", "AVAILABILITY_GROUP"}:
            warn(f"ServerPermissions row {i+2}: invalid Scope '{scope}'; skipped."); continue
        if perm not in SERVER_ALLOWED_PERMS.get(scope, set()):
            warn(f"ServerPermissions row {i+2}: Permission '{perm}' not allowed for Scope={scope}; skipped."); continue
        if scope == "ENDPOINT" and not endpoint:
            warn(f"ServerPermissions row {i+2}: EndpointName required for Scope=ENDPOINT; skipped."); continue
        if scope == "AVAILABILITY_GROUP" and not agname:
            warn(f"ServerPermissions row {i+2}: AGName required for Scope=AVAILABILITY_GROUP; skipped."); continue
        if state == "DENY" and wgo:
            warn(f"ServerPermissions row {i+2}: WithGrantOption ignored for DENY."); wgo = False

        sig = (login.lower(), state, scope, endpoint.lower(), agname.lower(), perm, wgo)
        if sig in seen:
            warn(f"ServerPermissions row {i+2}: duplicate row; de-duplicated."); continue
        seen.add(sig)
        rows.append({
            "LoginName": login, "State": state, "Scope": scope, "Permission": perm,
            "EndpointName": endpoint or None, "AGName": agname or None, "WithGrantOption": wgo
        })
    return rows

def load_server_role_memberships_df(df: pd.DataFrame) -> List[Dict]:
    require_columns(df, {"LoginName", "ServerRoleName"}, "ServerRoleMemberships")
    rows: List[Dict] = []
    seen: Set[Tuple] = set()
    for i, row in df.iterrows():
        login = norm(row.get("LoginName")); role = norm(row.get("ServerRoleName"))
        if not login or not role:
            warn(f"ServerRoleMemberships row {i+2}: missing LoginName/ServerRoleName; skipped."); continue
        if role.lower() not in SERVER_ROLES_ALLOWLIST:
            warn(f"ServerRoleMemberships row {i+2}: role '{role}' not in allow-list; skipped."); continue
        sig = (login.lower(), role.lower())
        if sig in seen:
            warn(f"ServerRoleMemberships row {i+2}: duplicate membership; de-duplicated."); continue
        seen.add(sig); rows.append({"LoginName": login, "ServerRoleName": role})
    return rows

def load_logins_df(df: pd.DataFrame) -> List[Dict]:
    require_columns(df, {"LoginName", "LoginType"}, "Logins")
    rows: List[Dict] = []
    seen: Set[str] = set()
    for i, row in df.iterrows():
        name = norm(row.get("LoginName"))
        ltype = norm(row.get("LoginType")).upper()
        if not name or not ltype:
            warn(f"Logins row {i+2}: missing LoginName/LoginType; skipped."); continue
        if ltype not in LOGIN_TYPES:
            warn(f"Logins row {i+2}: invalid LoginType '{ltype}'; skipped."); continue

        pwd_mode = norm(row.get("PasswordMode") or "PLAIN").upper()
        pwd = norm(row.get("Password"))
        pwdhash = norm(row.get("PasswordHash"))
        check_policy = as_bool(row.get("CheckPolicy"), True)
        check_exp = as_bool(row.get("CheckExpiration"), True)
        must_change = as_bool(row.get("MustChange"), False)
        default_db = norm(row.get("DefaultDatabase"))
        default_lang = norm(row.get("DefaultLanguage"))
        disabled = as_bool(row.get("Disabled"), False)
        sid = norm(row.get("SID"))

        if ltype == "SQL":
            if pwd_mode not in {"PLAIN", "HASHED"}:
                warn(f"Logins row {i+2}: PasswordMode must be PLAIN or HASHED for SQL logins; skipped."); continue
            if pwd_mode == "PLAIN" and not pwd:
                warn(f"Logins row {i+2}: Password required for SQL login with PLAIN mode; skipped."); continue
            if pwd_mode == "HASHED" and not pwdhash:
                warn(f"Logins row {i+2}: PasswordHash required for SQL login with HASHED mode; skipped."); continue
        else:
            pwd_mode, pwd, pwdhash, check_policy, check_exp, must_change = None, None, None, None, None, None

        key = name.lower()
        if key in seen:
            warn(f"Logins row {i+2}: duplicate login '{name}'; de-duplicated."); continue
        seen.add(key)
        rows.append({
            "LoginName": name, "LoginType": ltype, "PasswordMode": pwd_mode,
            "Password": pwd, "PasswordHash": pwdhash, "CheckPolicy": check_policy,
            "CheckExpiration": check_exp, "MustChange": must_change,
            "DefaultDatabase": default_db or None, "DefaultLanguage": default_lang or None,
            "Disabled": disabled, "SID": sid or None
        })
    return rows

def load_endpoints_df(df: pd.DataFrame) -> List[Dict]:
    require_columns(df, {"EndpointName", "EndpointType", "Port"}, "Endpoints")
    rows: List[Dict] = []
    seen: Set[str] = set()
    for i, row in df.iterrows():
        name = norm(row.get("EndpointName"))
        etype = norm(row.get("EndpointType")).upper()
        state = norm(row.get("State") or "STARTED").upper()
        role = norm(row.get("Role") or "ALL").upper()
        enc = norm(row.get("Encryption") or "REQUIRED").upper()
        alg = norm(row.get("Algorithm") or "AES").upper()
        auth = norm(row.get("Authentication") or "WINDOWS NEGOTIATE")
        owner = norm(row.get("OwnerLogin") or "sa")
        force = as_bool(row.get("ForceRecreate"), False)
        port_s = norm(row.get("Port"))
        try:
            port = int(port_s)
        except Exception:
            warn(f"Endpoints row {i+2}: invalid Port '{port_s}'; skipped."); continue

        if not name or etype != "DATABASE_MIRRORING":
            warn(f"Endpoints row {i+2}: only EndpointType=DATABASE_MIRRORING supported; skipped."); continue
        if state not in {"STARTED", "STOPPED"}:
            warn(f"Endpoints row {i+2}: invalid State '{state}'; defaulting to STARTED."); state = "STARTED"
        if role not in {"ALL", "PARTNER", "WITNESS", "PRIMARY", "SECONDARY"}:
            warn(f"Endpoints row {i+2}: invalid Role '{role}'; defaulting to ALL."); role = "ALL"
        if enc not in {"REQUIRED", "DISABLED"}:
            warn(f"Endpoints row {i+2}: invalid Encryption '{enc}'; defaulting to REQUIRED."); enc = "REQUIRED"
        if alg not in {"AES"}:
            warn(f"Endpoints row {i+2}: unsupported Algorithm '{alg}'; using AES."); alg = "AES"

        key = name.lower()
        if key in seen:
            warn(f"Endpoints row {i+2}: duplicate endpoint '{name}'; de-duplicated."); continue
        seen.add(key)
        rows.append({
            "EndpointName": name, "EndpointType": etype, "Port": port, "State": state,
            "Role": role, "Encryption": enc, "Algorithm": alg, "Authentication": auth,
            "OwnerLogin": owner, "ForceRecreate": force
        })
    return rows

# -----------------------
# DENY precedence heuristic (workbook-level)
# -----------------------

def detect_db_deny_conflicts(db_perms: List[Dict]) -> List[str]:
    grants = [p for p in db_perms if p["State"] == "GRANT"]
    denies = [p for p in db_perms if p["State"] == "DENY"]
    notes: List[str] = []
    deny_db = {(d["RoleName"].lower(), d["Permission"]) for d in denies if d["Scope"] == "DATABASE"}
    deny_schema = {(d["RoleName"].lower(), d["Permission"], (d["SchemaName"] or "").lower())
                   for d in denies if d["Scope"] == "SCHEMA"}
    deny_object = {(d["RoleName"].lower(), d["Permission"], (d["SchemaName"] or "").lower(), (d["ObjectName"] or "").lower())
                   for d in denies if d["Scope"] == "OBJECT"}
    for g in grants:
        r = g["RoleName"].lower(); p = g["Permission"]; s = g["Scope"]
        sch = (g["SchemaName"] or "").lower(); obj = (g["ObjectName"] or "").lower()
        if (r, p) in deny_db:
            notes.append(f"DB DENY overrides GRANT: Role={g['RoleName']}, Perm={p}, GrantScope={s}.")
        elif s in {"SCHEMA", "OBJECT"} and (r, p, sch) in deny_schema:
            notes.append(f"SCHEMA DENY overrides GRANT: Role={g['RoleName']}, Schema={g['SchemaName']}, Perm={p}, GrantScope={s}.")
        elif s == "OBJECT" and (r, p, sch, obj) in deny_object:
            notes.append(f"OBJECT DENY overrides GRANT: Role={g['RoleName']}, Obj={g['SchemaName']}.{g['ObjectName']}, Perm={p}.")
    return notes

def detect_server_deny_conflicts(sp: List[Dict]) -> List[str]:
    grants = [r for r in sp if r["State"] == "GRANT"]
    denies = [r for r in sp if r["State"] == "DENY"]
    notes: List[str] = []
    deny_server = {(d["LoginName"].lower(), d["Permission"]) for d in denies if d["Scope"] == "SERVER"}
    deny_ep = {(d["LoginName"].lower(), d["Permission"], (d["EndpointName"] or "").lower()) for d in denies if d["Scope"] == "ENDPOINT"}
    deny_ag = {(d["LoginName"].lower(), d["Permission"], (d["AGName"] or "").lower()) for d in denies if d["Scope"] == "AVAILABILITY_GROUP"}
    for g in grants:
        ln = g["LoginName"].lower(); p = g["Permission"]; sc = g["Scope"]
        if sc == "SERVER" and (ln, p) in deny_server:
            notes.append(f"SERVER DENY overrides GRANT: Login={g['LoginName']}, Perm={p}.")
        elif sc == "ENDPOINT" and (ln, p, (g['EndpointName'] or '').lower()) in deny_ep:
            notes.append(f"ENDPOINT DENY overrides GRANT: Login={g['LoginName']}, EP={g['EndpointName']}, Perm={p}.")
        elif sc == "AVAILABILITY_GROUP" and (ln, p, (g['AGName'] or '').lower()) in deny_ag:
            notes.append(f"AG DENY overrides GRANT: Login={g['LoginName']}, AG={g['AGName']}, Perm={p}.")
    return notes

# -----------------------
# Script builders (same as CSV version)
# -----------------------

def script_header(db_name_hint: Optional[str] = None) -> str:
    hdr = "-- Generated by sqlserver_security_suite_xlsx.py\nSET NOCOUNT ON;\nSET XACT_ABORT ON;\n"
    if db_name_hint:
        hdr += f"-- Target database hint: {db_name_hint}\n"
        hdr += f"USE [{db_name_hint}]\n"
    hdr += "GO\n"
    return hdr

def script_db_drop_all_memberships() -> str:
    return r"""
-- ==== Phase 1a (DB): DROP ALL ROLE MEMBERSHIPS (except implicit public) ====
BEGIN TRY
  BEGIN TRAN;
  DECLARE @debugging bit = """ + str(int(debugging)) + """; -- Set to 0 to EXECUTE, 1 to PRINT
  DECLARE @sql nvarchar(max) = N'';
  SELECT @sql = STRING_AGG(
      'ALTER ROLE ' + QUOTENAME(r.name) + ' DROP MEMBER ' + QUOTENAME(m.name) + ';',
      CHAR(10)
  )
  FROM sys.database_role_members drm
  JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
  JOIN sys.database_principals m ON m.principal_id = drm.member_principal_id
  WHERE r.name <> N'public';
  IF @debugging = 1 PRINT @sql;
  ELSE IF @sql IS NOT NULL AND LEN(@sql) > 0 EXEC sys.sp_executesql @sql;
  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH
GO
""".lstrip()

def script_db_revoke_all_permissions() -> str:
    return r"""
-- ==== Phase 1b (DB): REVOKE ALL EXPLICIT DATABASE PERMISSIONS ====
BEGIN TRY
  BEGIN TRAN;
  DECLARE @debugging bit = """ + str(int(debugging)) + """; -- Set to 0 to EXECUTE, 1 to PRINT
  DECLARE @revoke nvarchar(max) = N'';
  SELECT @revoke = STRING_AGG(CAST(
      'REVOKE ' + dp.permission_name COLLATE SQL_Latin1_General_CP1_CI_AS + ' ON ' +
      CASE dp.class
           WHEN 0 THEN 'DATABASE::' + QUOTENAME(DB_NAME())
           WHEN 1 THEN 'OBJECT::'  + QUOTENAME(OBJECT_SCHEMA_NAME(dp.major_id)) + '.' + QUOTENAME(OBJECT_NAME(dp.major_id))
           WHEN 3 THEN 'SCHEMA::'  + QUOTENAME(s.name)
           WHEN 5 THEN 'ASSEMBLY::' + QUOTENAME(a.name)
           WHEN 6 THEN 'TYPE::' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + '.' + QUOTENAME(t.name)
           ELSE 'DATABASE::' + QUOTENAME(DB_NAME())
      END
      + ' FROM ' + QUOTENAME(grantee.name) + ';' AS nvarchar(max)), CHAR(10))
  FROM sys.database_permissions dp
  JOIN sys.database_principals grantee ON grantee.principal_id = dp.grantee_principal_id
  LEFT JOIN sys.schemas s    ON dp.class = 3 AND s.schema_id    = dp.major_id
  LEFT JOIN sys.assemblies a ON dp.class = 5 AND a.assembly_id  = dp.major_id
  LEFT JOIN sys.types t      ON dp.class = 6 AND t.user_type_id = dp.major_id;
  IF @debugging = 1 PRINT @revoke
  ELSE IF @revoke IS NOT NULL AND LEN(@revoke) > 0 EXEC sys.sp_executesql @revoke;
  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH
GO
""".lstrip()

def tsql_in_list_str(names: Set[str]) -> str:
    def nl(s: str) -> str:
        return "N'" + s.replace("'", "''") + "'"
    return ", ".join(nl(n) for n in sorted(names, key=lambda s: s.lower()))

def script_server_clean(managed_logins: Set[str]) -> str:
    if not managed_logins:
        return "-- No managed logins discovered; skipping server-level cleanup.\n"
    inlist = tsql_in_list_str(managed_logins)
    return f"""
-- ==== Phase S1 (Server): DROP ALL SERVER ROLE MEMBERSHIPS for managed logins ====
BEGIN TRY
  BEGIN TRAN;
  DECLARE @debugging bit = """ + str(int(debugging)) + """; -- Set to 0 to EXECUTE, 1 to PRINT
  DECLARE @sql nvarchar(max) = N'';
  SELECT @sql = STRING_AGG(
      'ALTER SERVER ROLE ' + QUOTENAME(r.name) + ' DROP MEMBER ' + QUOTENAME(m.name) + ';',
      CHAR(10)
  )
  FROM sys.server_role_members srm
  JOIN sys.server_principals r ON r.principal_id = srm.role_principal_id
  JOIN sys.server_principals m ON m.principal_id = srm.member_principal_id
  WHERE m.name IN ({inlist});
  IF @debugging = 1 PRINT @sql;
  ELSE IF @sql IS NOT NULL AND LEN(@sql) > 0 EXEC (@sql);
  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH
GO

-- ==== Phase S1b (Server): REVOKE ALL EXPLICIT SERVER PERMISSIONS for managed logins ====
BEGIN TRY
  BEGIN TRAN;
  DECLARE @debugging bit = """ + str(int(debugging)) + """; -- Set to 0 to EXECUTE, 1 to PRINT
  DECLARE @revoke nvarchar(max) = N'';
  SELECT @revoke = STRING_AGG(CAST(
      'REVOKE ' + sp.permission_name COLLATE SQL_Latin1_General_CP1_CI_AS +
      CASE sp.class_desc
           WHEN 'ENDPOINT' THEN ' ON ENDPOINT::' + QUOTENAME(ep.name)
           WHEN 'AVAILABILITY_GROUP' THEN ' ON AVAILABILITY GROUP::' + QUOTENAME(ag.name)
           WHEN 'LOGIN' THEN ' ON LOGIN::' + QUOTENAME(lg.name)
           ELSE ''
      END
      + ' FROM ' + QUOTENAME(grantee.name) + ';' AS nvarchar(max)), CHAR(10))
  FROM sys.server_permissions sp
  JOIN sys.server_principals grantee ON grantee.principal_id = sp.grantee_principal_id
  LEFT JOIN sys.endpoints ep ON sp.class_desc = 'ENDPOINT' AND ep.endpoint_id = sp.major_id
  LEFT JOIN sys.server_principals lg ON sp.class_desc = 'LOGIN' AND lg.principal_id = sp.major_id
  LEFT JOIN sys.availability_groups ag ON sp.class_desc = 'AVAILABILITY_GROUP' AND ag.resource_id = sp.major_id
  WHERE grantee.name IN ({inlist});
  IF @debugging = 1 PRINT @revoke
  ELSE IF @revoke IS NOT NULL AND LEN(@revoke) > 0 EXEC (@revoke);
  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH
GO
""".lstrip()

def script_db_create_roles(roles: Set[str]) -> str:
    if not roles:
        return "-- No roles to create.\n"
    lines = ["-- ==== Phase 2 (DB): ENSURE ROLES EXIST ===="]
    for r in sorted(roles, key=lambda s: s.lower()):
        lines.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{r}' AND type = 'R')\n"
            f"    CREATE ROLE {bracket(r)} AUTHORIZATION [dbo];"
        )
    lines.append("GO")
    return "\n".join(lines) + "\n"

def script_db_create_users(users: List[Dict]) -> str:
    if not users:
        return "-- No users to create/ensure.\n"
    lines = ["-- ==== Phase 3 (DB): ENSURE USERS EXIST ===="]
    for u in users:
        uname = u["UserName"]
        defschema = u["DefaultSchema"]
        authtype = u["AuthType"]
        if authtype == "LOGIN":
            login = u["LoginName"]
            lines.append(
                f"IF SUSER_ID(N'{login}') IS NULL\n"
                f"    PRINT 'WARN: Login {login} not found; skipping CREATE USER for {uname}.';\n"
                f"ELSE IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{uname}')\n"
                f"    CREATE USER {bracket(uname)} FOR LOGIN {bracket(login)} WITH DEFAULT_SCHEMA = {bracket(defschema)};\n"
                f"ELSE\n"
                f"    ALTER USER {bracket(uname)} WITH DEFAULT_SCHEMA = {bracket(defschema)};"
            )
        elif authtype == "WITHOUT_LOGIN":
            lines.append(
                f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{uname}')\n"
                f"    CREATE USER {bracket(uname)} WITHOUT LOGIN WITH DEFAULT_SCHEMA = {bracket(defschema)};\n"
                f"ELSE\n"
                f"    ALTER USER {bracket(uname)} WITH DEFAULT_SCHEMA = {bracket(defschema)};"
            )
        else:  # EXTERNAL
            lines.append(
                f"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{uname}')\n"
                f"    CREATE USER {bracket(uname)} FROM EXTERNAL PROVIDER WITH DEFAULT_SCHEMA = {bracket(defschema)};\n"
                f"ELSE\n"
                f"    ALTER USER {bracket(uname)} WITH DEFAULT_SCHEMA = {bracket(defschema)};"
            )
        lines.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{defschema}')\n"
            f"    PRINT 'WARN: Default schema {defschema} not found; user {uname} created/updated without schema change.';"
        )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_db_permissions(perms: List[Dict]) -> str:
    if not perms: return "-- No DB permissions to apply.\n"
    lines = ["-- ==== Phase 4 (DB): APPLY GRANT/DENY PERMISSIONS ===="]
    for p in perms:
        action = p["State"]; role = bracket(p["RoleName"]); perm = p["Permission"]
        wgo = " WITH GRANT OPTION" if (action == "GRANT" and p["WithGrantOption"]) else ""
        if p["Scope"] == "DATABASE":
            lines.append(f"{action} {perm} TO {role}{wgo};")
        elif p["Scope"] == "SCHEMA":
            schema = p["SchemaName"]
            lines.append(
                f"IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}')\n"
                f"    {action} {perm} ON SCHEMA::{bracket(schema)} TO {role}{wgo};\n"
                f"ELSE PRINT 'WARN: Schema {schema} not found; skipping {action} to role {p['RoleName']}.';"
            )
        else:
            schema = p["SchemaName"]; obj = p["ObjectName"]
            lines.append(
                "IF EXISTS (SELECT 1 FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id "
                f"WHERE s.name=N'{schema}' AND o.name=N'{obj}')\n"
                f"    {action} {perm} ON OBJECT::{bracket(schema)}.{bracket(obj)} TO {role}{wgo};\n"
                f"ELSE PRINT 'WARN: Object {schema}.{obj} not found; skipping {action} to role {p['RoleName']}.';"
            )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_db_memberships(members: List[Dict]) -> str:
    if not members: return "-- No DB role memberships to add.\n"
    lines = ["-- ==== Phase 5 (DB): ADD ROLE MEMBERSHIPS ===="]
    for m in members:
        user = m["UserName"]; role = m["RoleName"]
        lines.append(
            f"IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{user}')"
            f" AND EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{role}' AND type='R')\n"
            f"    ALTER ROLE {bracket(role)} ADD MEMBER {bracket(user)};\n"
            f"ELSE PRINT 'WARN: Skipping membership; user or role missing (User={user}, Role={role}).';"
        )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_server_create_logins(logins: List[Dict]) -> str:
    if not logins: return "-- No logins to create.\n"
    lines = ["-- ==== Phase S0 (Server): ENSURE LOGINS EXIST ===="]
    for l in logins:
        name = l["LoginName"]; ltype = l["LoginType"]
        dd = l["DefaultDatabase"]; dl = l["DefaultLanguage"]
        disabled = l["Disabled"]; sid = l["SID"]
        if ltype == "SQL":
            mode = l["PasswordMode"]
            opts = []
            if mode == "PLAIN":
                opts.append(f"PASSWORD = {nliteral(l['Password'])}")
                if l["MustChange"]:
                    opts.append("MUST_CHANGE")
                if l["CheckPolicy"] is not None:
                    opts.append(f"CHECK_POLICY = {'ON' if l['CheckPolicy'] else 'OFF'}")
                if l["CheckExpiration"] is not None:
                    opts.append(f"CHECK_EXPIRATION = {'ON' if l['CheckExpiration'] else 'OFF'}")
            else:  # HASHED
                opts.append(f"PASSWORD = {l['PasswordHash']} HASHED")
                if l["CheckPolicy"] is not None:
                    opts.append(f"CHECK_POLICY = {'ON' if l['CheckPolicy'] else 'OFF'}")
                if l["CheckExpiration"] is not None:
                    opts.append(f"CHECK_EXPIRATION = {'ON' if l['CheckExpiration'] else 'OFF'}")
            if dd: opts.append(f"DEFAULT_DATABASE = {bracket(dd)}")
            if dl: opts.append(f"DEFAULT_LANGUAGE = {bracket(dl)}")
            if sid: opts.append(f"SID = {sid}")

            lines.append(
                f"IF SUSER_ID(N'{name}') IS NULL\n"
                f"    CREATE LOGIN {bracket(name)} WITH {', '.join(opts)};\n"
                f"ELSE\n"
                f"    PRINT 'INFO: Login {name} exists; skipping CREATE LOGIN.';"
            )
            if disabled:
                lines.append(f"ALTER LOGIN {bracket(name)} DISABLE;")
        elif ltype == "WINDOWS":
            opts = []
            opts.append("FROM WINDOWS")
            if dd: opts.append(f"DEFAULT_DATABASE = {bracket(dd)}")
            if dl: opts.append(f"DEFAULT_LANGUAGE = {bracket(dl)}")
            if sid: opts.append(f"SID = {sid}")
            lines.append(
                f"IF SUSER_ID(N'{name}') IS NULL\n"
                f"    CREATE LOGIN {bracket(name)} FROM WINDOWS { 'WITH ' + ', '.join(opts[1:]) if len(opts)>1 else '' };\n"
                f"ELSE\n"
                f"    PRINT 'INFO: Login {name} exists; skipping CREATE LOGIN.';"
            )
            if disabled:
                lines.append(f"ALTER LOGIN {bracket(name)} DISABLE;")
        else:  # EXTERNAL
            lines.append(
                f"IF SUSER_ID(N'{name}') IS NULL\n"
                f"    CREATE LOGIN {bracket(name)} FROM EXTERNAL PROVIDER;\n"
                f"ELSE\n"
                f"    PRINT 'INFO: Login {name} exists; skipping CREATE LOGIN.';"
            )
            if disabled:
                lines.append(f"ALTER LOGIN {bracket(name)} DISABLE;")
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_server_create_endpoints(eps: List[Dict]) -> str:
    if not eps: return "-- No endpoints to create.\n"
    lines = ["-- ==== Phase S0b (Server): ENSURE ENDPOINTS EXIST ===="]
    for e in eps:
        name = e["EndpointName"]; port = e["Port"]
        state = e["State"]; role = e["Role"]; enc = e["Encryption"]
        alg = e["Algorithm"]; auth = e["Authentication"]; owner = e["OwnerLogin"]
        force = e["ForceRecreate"]
        lines.append(
            f"IF EXISTS (SELECT 1 FROM sys.endpoints WHERE name = N'{name}')\n"
            f"BEGIN\n"
            f"    PRINT 'INFO: Endpoint {name} exists.';\n"
            f"    {'ALTER ENDPOINT ' + bracket(name) + ' STATE = STOPPED; DROP ENDPOINT ' + bracket(name) + ';' if force else 'PRINT ''INFO: ForceRecreate not set; preserving existing endpoint.'';'}\n"
            f"END\n"
            f"ELSE PRINT 'INFO: Endpoint {name} not found; will create.';"
        )
        lines.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.endpoints WHERE name = N'{name}')\n"
            f"BEGIN\n"
            f"    CREATE ENDPOINT {bracket(name)} STATE = {state}\n"
            f"    AS TCP (LISTENER_PORT = {port}, LISTENER_IP = ALL)\n"
            f"    FOR DATABASE_MIRRORING (ROLE = {role}, AUTHENTICATION = {auth}, ENCRYPTION = {enc} ALGORITHM {alg});\n"
            f"    ALTER AUTHORIZATION ON ENDPOINT::{bracket(name)} TO {bracket(owner)};\n"
            f"END\n"
        )
        lines.append(
            f"IF EXISTS (SELECT 1 FROM sys.endpoints WHERE name = N'{name}')\n"
            f"    ALTER ENDPOINT {bracket(name)} STATE = {state};"
        )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_server_permissions(sp: List[Dict]) -> str:
    if not sp: return "-- No server permissions to apply.\n"
    lines = ["-- ==== Phase S2 (Server): APPLY GRANT/DENY PERMISSIONS ===="]
    for row in sp:
        login = row["LoginName"]; action = row["State"]; perm = row["Permission"]
        wgo = " WITH GRANT OPTION" if (action == "GRANT" and row["WithGrantOption"]) else ""
        sc = row["Scope"]
        if sc == "SERVER":
            lines.append(
                f"IF SUSER_ID(N'{login}') IS NOT NULL\n"
                f"    {action} {perm} TO {bracket(login)}{wgo};\n"
                f"ELSE PRINT 'WARN: Login {login} not found; skipping {action} {perm}.';"
            )
        elif sc == "ENDPOINT":
            ep = row["EndpointName"]
            lines.append(
                f"IF EXISTS (SELECT 1 FROM sys.endpoints WHERE name = N'{ep}') AND SUSER_ID(N'{login}') IS NOT NULL\n"
                f"    {action} {perm} ON ENDPOINT::{bracket(ep)} TO {bracket(login)}{wgo};\n"
                f"ELSE PRINT 'WARN: Endpoint {ep} or login {login} not found; skipping {action} {perm}.';"
            )
        else:  # AVAILABILITY_GROUP
            ag = row["AGName"]
            lines.append(
                f"IF EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = N'{ag}') AND SUSER_ID(N'{login}') IS NOT NULL\n"
                f"    {action} {perm} ON AVAILABILITY GROUP::{bracket(ag)} TO {bracket(login)}{wgo};\n"
                f"ELSE PRINT 'WARN: Availability Group {ag} or login {login} not found; skipping {action} {perm}.';"
            )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_server_role_memberships(srm: List[Dict]) -> str:
    if not srm: return "-- No server role memberships to add.\n"
    lines = ["-- ==== Phase S3 (Server): ADD SERVER ROLE MEMBERSHIPS ===="]
    for row in srm:
        login = row["LoginName"]; role = row["ServerRoleName"]
        lines.append(
            f"IF SUSER_ID(N'{login}') IS NOT NULL AND EXISTS (SELECT 1 FROM sys.server_principals WHERE name=N'{role}' AND type='R')\n"
            f"    ALTER SERVER ROLE {bracket(role)} ADD MEMBER {bracket(login)};\n"
            f"ELSE PRINT 'WARN: Skipping server role membership; login or role missing (Login={login}, Role={role}).';"
        )
    lines.append("GO"); return "\n".join(lines) + "\n"

def script_conflict_reports() -> str:
    return r"""
-- ==== Diagnostics: Potential GRANTs overridden by DENYs (database) ====
;WITH D AS (
  SELECT dp.class, dp.major_id, dp.minor_id, dp.grantee_principal_id, dp.permission_name
  FROM sys.database_permissions dp WHERE dp.state_desc='DENY'
),
G AS (
  SELECT dp.class, dp.major_id, dp.minor_id, dp.grantee_principal_id, dp.permission_name
  FROM sys.database_permissions dp WHERE dp.state_desc='GRANT'
)
SELECT DISTINCT
  grantee = dp2.name,
  deny_permission = D.permission_name,
  deny_class = D.class,
  grant_permission = G.permission_name,
  grant_class = G.class
FROM D
JOIN sys.database_principals dp2 ON dp2.principal_id = D.grantee_principal_id
LEFT JOIN G ON G.grantee_principal_id = D.grantee_principal_id
           AND G.permission_name = D.permission_name
           AND (
                (D.class = 1 AND G.class = 1 AND D.major_id = G.major_id AND D.minor_id = G.minor_id)
                OR (D.class = 3 AND G.class = 1 AND D.major_id = G.major_id)
                OR (D.class = 0)
           )
WHERE G.grantee_principal_id IS NOT NULL;

-- ==== Diagnostics: Potential GRANTs overridden by DENYs (server) ====
;WITH Ds AS (
  SELECT sp.class_desc, sp.major_id, sp.grantee_principal_id, sp.permission_name
  FROM sys.server_permissions sp WHERE sp.state_desc = 'DENY'
),
Gs AS (
  SELECT sp.class_desc, sp.major_id, sp.grantee_principal_id, sp.permission_name
  FROM sys.server_permissions sp WHERE sp.state_desc = 'GRANT'
)
SELECT DISTINCT
  grantee = sp2.name,
  deny_permission = Ds.permission_name,
  deny_class = Ds.class_desc,
  grant_permission = Gs.permission_name,
  grant_class = Gs.class_desc
FROM Ds
JOIN sys.server_principals sp2 ON sp2.principal_id = Ds.grantee_principal_id
LEFT JOIN Gs ON Gs.grantee_principal_id = Ds.grantee_principal_id
            AND Gs.permission_name = Ds.permission_name
            AND (
                 (Ds.class_desc = Gs.class_desc AND Ds.major_id = Gs.major_id)
                 OR (Ds.class_desc = 'SERVER')
            )
WHERE Gs.grantee_principal_id IS NOT NULL;
GO
""".lstrip()

def build_full_script(roles: Set[str], users: List[Dict],
                      db_perms: List[Dict], db_membs: List[Dict],
                      server_perms: List[Dict], server_membs: List[Dict],
                      logins: List[Dict], endpoints: List[Dict],
                      db_name_hint: Optional[str] = None) -> str:
    managed_logins: Set[str] = set()
    managed_logins.update([u["LoginName"] for u in users if u["AuthType"] == "LOGIN" and u["LoginName"]])
    managed_logins.update([r["LoginName"] for r in server_perms])
    managed_logins.update([r["LoginName"] for r in server_membs])
    managed_logins.update([l["LoginName"] for l in logins])

    deny_notes = []
    deny_notes += detect_db_deny_conflicts(db_perms)
    deny_notes += detect_server_deny_conflicts(server_perms)

    notes_section = ""
    if deny_notes:
        notes_section = "-- ==== Pre-run DENY precedence notes (from workbook analysis) ====\n" + \
                        "\n".join(f"-- NOTE: {n}" for n in deny_notes) + "\nGO\n"

    return (
        script_header(db_name_hint) +
        notes_section +
        script_db_drop_all_memberships() +
        script_db_revoke_all_permissions() +
        script_server_clean(managed_logins) +
        script_server_create_logins(logins) +
        script_server_create_endpoints(endpoints) +
        script_db_create_roles(roles) +
        script_db_create_users(users) +
        script_db_permissions(db_perms) +
        script_db_memberships(db_membs) +
        script_server_permissions(server_perms) +
        script_server_role_memberships(server_membs) +
        script_conflict_reports() +
        "-- ==== Done ====\n"
    )

# -----------------------
# Optional execution
# -----------------------

def maybe_execute(conn_str: str, script: str):
    try:
        import pyodbc  # optional dependency
    except Exception as e:
        warn(f"pyodbc not available ({e}); skipping execution."); return
    info("Connecting with pyodbc …")
    cn = pyodbc.connect(conn_str, autocommit=True)
    cur = cn.cursor()
    batches = [b.strip() for b in script.split("\nGO") if b.strip()]
    info(f"Executing {len(batches)} batches …")
    for i, batch in enumerate(batches, start=1):
        try:
            cur.execute(batch); info(f"Batch {i}/{len(batches)} OK")
        except Exception as ex:
            warn(f"Execution error in batch {i}: {ex}"); raise
    cur.close(); cn.close(); info("Execution complete.")

# -----------------------
# Main
# -----------------------

def main():
    ap = argparse.ArgumentParser(description="SQL Server security from one XLSX (from-scratch rebuild: DB + Server + Logins + Endpoints).")
    ap.add_argument("--xlsx", required=True, help="Path to Excel workbook containing all sheets.")
    ap.add_argument("--out-sql", default="security_rebuild.sql", help="Output SQL filename.")
    ap.add_argument("--debugging", action="store_true", help="Enable debugging mode.")
    ap.add_argument("--execute", action="store_true", help="Execute the generated script via pyodbc.")
    ap.add_argument("--conn-str", default=None, help="pyodbc connection string.")
    ap.add_argument("--db-hint", default=None, help="Optional database name hint to include in script header.")
    args = ap.parse_args()

    xlsx_path = args.xlsx
    debugging = args.debugging

    # Load workbook
    info(f"Loading workbook: {xlsx_path}")
    try:
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    except Exception as e:
        sys.exit(f"ERROR: Failed to open workbook '{xlsx_path}': {e}")

    # Required sheets
    required_sheets = ["Roles", "Users", "Permissions", "Memberships"]
    required_sheets = []
    for s in required_sheets:
        if s not in xls.sheet_names:
            sys.exit(f"ERROR: Required sheet '{s}' is missing from workbook. Found sheets: {xls.sheet_names}")

    # Read sheets (optional ones if present)
    roles_df = pd.read_excel(xls, sheet_name="Roles").fillna("") if "Roles" in xls.sheet_names else pd.DataFrame()
    users_df = pd.read_excel(xls, sheet_name="Users").fillna("") if "Users" in xls.sheet_names else pd.DataFrame()
    perms_df = pd.read_excel(xls, sheet_name="Permissions").fillna("") if "Permissions" in xls.sheet_names else pd.DataFrame()
    memb_df  = pd.read_excel(xls, sheet_name="Memberships").fillna("") if "Memberships" in xls.sheet_names else pd.DataFrame()

    server_perms_df = pd.read_excel(xls, sheet_name="ServerPermissions").fillna("") if "ServerPermissions" in xls.sheet_names else pd.DataFrame()
    server_membs_df = pd.read_excel(xls, sheet_name="ServerRoleMemberships").fillna("") if "ServerRoleMemberships" in xls.sheet_names else pd.DataFrame()
    logins_df = pd.read_excel(xls, sheet_name="Logins").fillna("") if "Logins" in xls.sheet_names else pd.DataFrame()
    endpoints_df = pd.read_excel(xls, sheet_name="Endpoints").fillna("") if "Endpoints" in xls.sheet_names else pd.DataFrame()

    # Phase: load & validate
    info("Phase: Load & validate sheets …")
    roles = load_roles_df(roles_df) if not roles_df.empty else set()
    users = load_users_df(users_df) if not users_df.empty else set()
    db_perms = load_db_permissions_df(perms_df, roles) if not perms_df.empty else []
    db_membs = load_db_memberships_df(memb_df, roles) if not memb_df.empty else []
    server_perms = load_server_permissions_df(server_perms_df) if not server_perms_df.empty else []
    server_membs = load_server_role_memberships_df(server_membs_df) if not server_membs_df.empty else []
    logins = load_logins_df(logins_df) if not logins_df.empty else []
    endpoints = load_endpoints_df(endpoints_df) if not endpoints_df.empty else []

    info(f"Validation OK: {len(roles)} DB role(s), {len(users)} user(s), "
         f"{len(db_perms)} DB permission row(s), {len(db_membs)} DB membership(s), "
         f"{len(server_perms)} server permission row(s), {len(server_membs)} server membership(s), "
         f"{len(logins)} login(s), {len(endpoints)} endpoint(s). "
         f"Next: build removal script.")

    # Build script
    script = build_full_script(roles, users, db_perms, db_membs, server_perms, server_membs, logins, endpoints, db_name_hint=args.db_hint)

    # Write
    out_path = args.out_sql
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(script)
    info(f"Script written to: {out_path}")
    info("Sanity check: script includes DB cleanup, scoped server cleanup, logins/endpoints, and rebuild phases. Next: optional execution.")

    # Optional execution
    if args.execute:
        if not args.conn_str:
            sys.exit("ERROR: --execute requires --conn-str")
        info("Executing the generated script …")
        maybe_execute(args.conn_str, script)
        info("Post-check: Review SQL PRINT warnings and diagnostics.")
    else:
        info("Dry run only. Re-run with --execute --conn-str '<pyodbc-connection-string>' to apply.")

if __name__ == "__main__":
    main()
