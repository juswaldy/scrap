"""Parse SQL Server execution plan XML and generate Mermaid diagrams in Markdown."""

import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from dataclasses import dataclass, field

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


@dataclass
class PlanNode:
    node_id: int
    physical_op: str
    logical_op: str
    est_rows: float
    actual_rows: int = 0
    actual_rows_read: int = 0
    actual_elapsed_ms: int = 0
    actual_cpu_ms: int = 0
    actual_executions: int = 0
    actual_logical_reads: int = 0
    actual_physical_reads: int = 0
    est_subtree_cost: float = 0.0
    table_name: str = ""
    index_name: str = ""
    is_lookup: bool = False
    children: list = field(default_factory=list)
    cost_pct: float = 0.0
    self_ms: int = 0
    self_pct: float = 0.0


def _get_op_element(relop_el, ns):
    """Get the immediate operator element (first non-metadata child)."""
    skip = {"OutputList", "RunTimeInformation", "MemoryFractions", "Warnings"}
    for child in relop_el:
        tag = child.tag.replace(f"{{{ns}}}", "")
        if tag not in skip:
            return child
    return None


def _find_child_relops(relop_el, ns) -> list:
    """Find direct child RelOp elements belonging to this operator (not deeper nested)."""
    children = []
    op_el = _get_op_element(relop_el, ns)
    if op_el is None:
        return children

    # BFS at most 3 levels inside the operator element, stopping at RelOp
    queue = list(op_el)
    while queue:
        el = queue.pop(0)
        tag = el.tag.replace(f"{{{ns}}}", "")
        if tag == "RelOp":
            children.append(el)
        else:
            queue.extend(list(el))
    return children


def parse_relop(el, ns) -> PlanNode:
    """Recursively parse a RelOp element into a PlanNode tree."""
    node = PlanNode(
        node_id=int(el.get("NodeId", -1)),
        physical_op=el.get("PhysicalOp", ""),
        logical_op=el.get("LogicalOp", ""),
        est_rows=float(el.get("EstimateRows", 0)),
        est_subtree_cost=float(el.get("EstimatedTotalSubtreeCost", 0)),
    )

    # Runtime counters — only from direct RunTimeInformation child of this RelOp
    for rti in el.findall(f"{{{ns}}}RunTimeInformation"):
        for rt in rti.findall(f"{{{ns}}}RunTimeCountersPerThread"):
            node.actual_rows += int(rt.get("ActualRows", 0))
            node.actual_rows_read += int(rt.get("ActualRowsRead", 0))
            node.actual_elapsed_ms = max(node.actual_elapsed_ms, int(rt.get("ActualElapsedms", 0)))
            node.actual_cpu_ms += int(rt.get("ActualCPUms", 0))
            node.actual_executions += int(rt.get("ActualExecutions", 0))
            node.actual_logical_reads += int(rt.get("ActualLogicalReads", 0))
            node.actual_physical_reads += int(rt.get("ActualPhysicalReads", 0))

    # Table/Index info — only from this operator's own element, not child RelOps
    op_el = _get_op_element(el, ns)
    if op_el is not None:
        # Find Object in the operator element but NOT inside child RelOps
        _find_object_in_op(op_el, ns, node)

    # Recurse into child RelOps
    for child_relop in _find_child_relops(el, ns):
        node.children.append(parse_relop(child_relop, ns))

    return node


def _find_object_in_op(el, ns, node: PlanNode):
    """Find Object and IndexScan attributes in op element, stopping at child RelOps."""
    tag = el.tag.replace(f"{{{ns}}}", "")
    if tag == "RelOp":
        return  # Don't descend into child RelOps
    if tag == "Object" and el.get("Table"):
        node.table_name = el.get("Table", "").strip("[]")
        node.index_name = el.get("Index", "").strip("[]")
    if tag == "IndexScan" and el.get("Lookup") == "true":
        node.is_lookup = True
    for child in el:
        _find_object_in_op(child, ns, node)


def collect_all_nodes(node: PlanNode) -> list[PlanNode]:
    """Flatten the tree into a list."""
    result = [node]
    for child in node.children:
        result.extend(collect_all_nodes(child))
    return result


def compute_cost_pct(root: PlanNode):
    """Compute cost % for each node based on ActualElapsedms relative to max across all nodes.
    Also compute self_ms (exclusive time) using max descendant elapsed."""
    all_nodes = collect_all_nodes(root)

    # Propagate elapsed times upward through nodes with 0ms that have children with actual times
    _propagate_elapsed(root)

    max_ms = max((n.actual_elapsed_ms for n in all_nodes), default=0)
    if max_ms == 0:
        return

    # Compute max descendant elapsed for each node (post-order)
    _compute_max_descendant(root)

    for n in all_nodes:
        n.cost_pct = (n.actual_elapsed_ms / max_ms) * 100
        n.self_pct = (n.self_ms / max_ms) * 100


def _compute_max_descendant(node: PlanNode) -> int:
    """Compute self_ms = elapsed - max descendant elapsed. Returns max elapsed in subtree."""
    if not node.children:
        node.self_ms = node.actual_elapsed_ms
        return node.actual_elapsed_ms

    max_desc = 0
    for child in node.children:
        max_desc = max(max_desc, _compute_max_descendant(child))

    node.self_ms = max(0, node.actual_elapsed_ms - max_desc)
    return max(node.actual_elapsed_ms, max_desc)


def _propagate_elapsed(node: PlanNode):
    """Propagate elapsed times upward through nodes with 0ms elapsed that have children."""
    for child in node.children:
        _propagate_elapsed(child)
    if node.actual_elapsed_ms == 0 and node.children:
        node.actual_elapsed_ms = max(c.actual_elapsed_ms for c in node.children)


def sanitize_label(text: str) -> str:
    """Escape characters problematic in Mermaid labels."""
    return text.replace('"', "'").replace("[", "").replace("]", "").replace("<", "lt").replace(">", "gt")


def node_label(n: PlanNode) -> str:
    """Build a short label for the Mermaid node."""
    parts = [f"#{n.node_id} {n.physical_op}"]
    if n.table_name:
        tbl = n.table_name
        if n.index_name:
            tbl += f".{n.index_name}"
        if n.is_lookup:
            tbl += " (Lookup)"
        parts.append(tbl)
    parts.append(f"Rows: {n.actual_rows:,}")
    parts.append(f"Cost: {n.cost_pct:.1f}%")
    if n.self_ms:
        parts.append(f"Self: {n.self_ms:,}ms ({n.self_pct:.1f}%)")
    return sanitize_label(" | ".join(parts))


def build_mermaid_edges(node: PlanNode, edges: list, threshold_pct: float = 0.0):
    """Generate Mermaid edges from the plan tree. Only include nodes >= threshold_pct."""
    for child in node.children:
        if child.cost_pct >= threshold_pct and node.cost_pct >= threshold_pct:
            edges.append((child.node_id, node.node_id))
        build_mermaid_edges(child, edges, threshold_pct)


def collect_nodes_for_diagram(node: PlanNode, threshold_pct: float = 0.0) -> list[PlanNode]:
    """Collect nodes that meet the threshold for diagram inclusion."""
    result = []
    for n in collect_all_nodes(node):
        if n.cost_pct >= threshold_pct:
            result.append(n)
    return result


def generate_mermaid(root: PlanNode, stmt_idx: int, threshold_pct: float = 0.0) -> str:
    """Generate a Mermaid flowchart for a statement's plan."""
    nodes = collect_nodes_for_diagram(root, threshold_pct)
    if not nodes:
        return ""

    edges = []
    build_mermaid_edges(root, edges, threshold_pct)

    # Also connect nodes that are in the diagram but whose direct parent was filtered out
    node_ids_in_diagram = {n.node_id for n in nodes}
    # Build parent map from full tree
    parent_map = {}
    def _map_parents(n: PlanNode):
        for c in n.children:
            parent_map[c.node_id] = n.node_id
            _map_parents(c)
    _map_parents(root)

    # For threshold diagrams, connect child to nearest ancestor in diagram
    if threshold_pct > 0:
        edges = []
        for n in nodes:
            nid = n.node_id
            pid = parent_map.get(nid)
            while pid is not None and pid not in node_ids_in_diagram:
                pid = parent_map.get(pid)
            if pid is not None and pid in node_ids_in_diagram:
                edges.append((nid, pid))

    lines = [f"flowchart BT"]
    # Define nodes
    for n in nodes:
        label = node_label(n)
        nid = f"S{stmt_idx}N{n.node_id}"
        if n.self_pct >= 5:
            lines.append(f'    {nid}["{label}"]:::hot')
        elif n.cost_pct >= 50:
            lines.append(f'    {nid}["{label}"]:::warm')
        else:
            lines.append(f'    {nid}["{label}"]')

    # Define edges (data flows bottom-to-top: child produces rows for parent)
    for child_id, parent_id in edges:
        lines.append(f"    S{stmt_idx}N{child_id} --> S{stmt_idx}N{parent_id}")

    lines.append("    classDef hot fill:#ff4444,color:#fff,stroke:#cc0000")
    lines.append("    classDef warm fill:#ff9944,color:#fff,stroke:#cc6600")

    return "\n".join(lines)


def _generate_mermaid_subset(root: PlanNode, stmt_idx: int, node_ids: set) -> str:
    """Generate a Mermaid diagram for a specific subset of nodes, connected to nearest ancestors."""
    all_nodes = collect_all_nodes(root)
    nodes = [n for n in all_nodes if n.node_id in node_ids]
    if not nodes:
        return ""

    # Build parent map
    parent_map = {}
    def _map_parents(n: PlanNode):
        for c in n.children:
            parent_map[c.node_id] = n.node_id
            _map_parents(c)
    _map_parents(root)

    edges = []
    for n in nodes:
        nid = n.node_id
        pid = parent_map.get(nid)
        while pid is not None and pid not in node_ids:
            pid = parent_map.get(pid)
        if pid is not None and pid in node_ids:
            edges.append((nid, pid))

    lines = ["flowchart BT"]
    for n in nodes:
        label = node_label(n)
        nid = f"S{stmt_idx}N{n.node_id}"
        if n.self_pct >= 5:
            lines.append(f'    {nid}["{label}"]:::hot')
        else:
            lines.append(f'    {nid}["{label}"]:::warm')

    for child_id, parent_id in edges:
        lines.append(f"    S{stmt_idx}N{child_id} --> S{stmt_idx}N{parent_id}")

    lines.append("    classDef hot fill:#ff4444,color:#fff,stroke:#cc0000")
    lines.append("    classDef warm fill:#ff9944,color:#fff,stroke:#cc6600")
    return "\n".join(lines)


def extract_warnings(stmt_el, ns) -> list[str]:
    """Extract warnings from the query plan."""
    warnings = []
    for w in stmt_el.findall(f".//{{{ns}}}Warnings/*"):
        tag = w.tag.replace(f"{{{ns}}}", "")
        attrs = " | ".join(f"{k}={v}" for k, v in w.attrib.items())
        warnings.append(f"- **{tag}**: {attrs}")
    return warnings


def extract_missing_indexes(stmt_el, ns) -> list[str]:
    """Extract missing index suggestions."""
    suggestions = []
    for group in stmt_el.findall(f".//{{{ns}}}MissingIndexGroup"):
        impact = group.get("Impact", "?")
        for mi in group.findall(f"{{{ns}}}MissingIndex"):
            db = mi.get("Database", "").strip("[]")
            schema = mi.get("Schema", "").strip("[]")
            table = mi.get("Table", "").strip("[]")

            eq_cols, ineq_cols, inc_cols = [], [], []
            for cg in mi.findall(f"{{{ns}}}ColumnGroup"):
                usage = cg.get("Usage", "")
                cols = [c.get("Name", "").strip("[]") for c in cg.findall(f"{{{ns}}}Column")]
                if usage == "EQUALITY":
                    eq_cols = cols
                elif usage == "INEQUALITY":
                    ineq_cols = cols
                elif usage == "INCLUDE":
                    inc_cols = cols

            parts = [f"  - **Table**: `{db}.{schema}.{table}` (Impact: {impact}%)"]
            if eq_cols:
                parts.append(f"    - Equality: `{', '.join(eq_cols)}`")
            if ineq_cols:
                parts.append(f"    - Inequality: `{', '.join(ineq_cols)}`")
            if inc_cols:
                parts.append(f"    - Include: `{', '.join(inc_cols)}`")
            suggestions.append("\n".join(parts))
    return suggestions


def extract_wait_stats(stmt_el, ns) -> list[str]:
    """Extract wait statistics."""
    waits = []
    for w in stmt_el.findall(f".//{{{ns}}}Wait"):
        wtype = w.get("WaitType", "?")
        wms = w.get("WaitTimeMs", "0")
        wcount = w.get("WaitCount", "0")
        waits.append(f"| {wtype} | {wms} | {wcount} |")
    return waits


def process_plan(xml_path: str) -> str:
    """Main entry: parse XML, generate Markdown with Mermaid diagrams."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    md_parts = [f"# SQL Execution Plan Analysis\n"]
    md_parts.append(f"**Source**: `{Path(xml_path).name}`\n")

    stmt_idx = 0
    for batch_idx, batch in enumerate(root.findall(f".//{{{NS}}}Batch")):
        for stmt in batch.findall(f".//{{{NS}}}StmtSimple"):
            stmt_idx += 1
            stmt_text = stmt.get("StatementText", "").replace("\r\n", "\n").strip()
            stmt_type = stmt.get("StatementType", "?")
            stmt_cost = stmt.get("StatementSubTreeCost", "?")

            md_parts.append(f"---\n## Statement {stmt_idx} ({stmt_type})\n")
            # Truncate very long SQL
            display_sql = stmt_text[:500] + ("..." if len(stmt_text) > 500 else "")
            md_parts.append(f"```sql\n{display_sql}\n```\n")

            # Query plan metadata
            qp = stmt.find(f".//{{{NS}}}QueryPlan")
            if qp is None:
                md_parts.append("*No query plan found.*\n")
                continue

            elapsed = ""
            qt = qp.find(f"{{{NS}}}QueryTimeStats")
            if qt is not None:
                elapsed = qt.get("ElapsedTime", "0")
                cpu = qt.get("CpuTime", "0")
                md_parts.append(f"- **Elapsed**: {int(elapsed):,}ms | **CPU**: {int(cpu):,}ms | **Est. Cost**: {stmt_cost}\n")

            dop = qp.get("DegreeOfParallelism", "?")
            mem = qp.get("MemoryGrant", "")
            no_par = qp.get("NonParallelPlanReason", "")
            details = [f"- **DOP**: {dop}"]
            if mem:
                mem_info = qp.find(f"{{{NS}}}MemoryGrantInfo")
                if mem_info is not None:
                    granted = int(mem_info.get("GrantedMemory", 0))
                    used = int(mem_info.get("MaxUsedMemory", 0))
                    details.append(f"- **Memory Grant**: {granted:,} KB (Used: {used:,} KB, {used*100//max(granted,1)}%)")
            if no_par:
                details.append(f"- **Non-Parallel Reason**: {no_par}")
            md_parts.append("\n".join(details) + "\n")

            # Warnings
            warnings = extract_warnings(stmt, NS)
            if warnings:
                md_parts.append("### Warnings\n")
                md_parts.append("\n".join(warnings) + "\n")

            # Missing Indexes
            missing = extract_missing_indexes(stmt, NS)
            if missing:
                md_parts.append("### Missing Index Suggestions\n")
                md_parts.append("\n".join(missing) + "\n")

            # Wait Stats
            waits = extract_wait_stats(stmt, NS)
            if waits:
                md_parts.append("### Wait Statistics\n")
                md_parts.append("| Wait Type | Time (ms) | Count |")
                md_parts.append("|-----------|-----------|-------|")
                md_parts.append("\n".join(waits) + "\n")

            # Parse the operator tree
            root_relop = qp.find(f"{{{NS}}}RelOp")
            if root_relop is None:
                continue

            plan_root = parse_relop(root_relop, NS)
            compute_cost_pct(plan_root)
            all_nodes = collect_all_nodes(plan_root)

            # Full diagram
            md_parts.append("### Full Execution Plan\n")
            full_mermaid = generate_mermaid(plan_root, stmt_idx, threshold_pct=0.0)
            if full_mermaid:
                md_parts.append(f"```mermaid\n{full_mermaid}\n```\n")

            # Problematic nodes: identify by highest exclusive (self) time
            # Nodes where actual work happens, not just cumulative join operators
            sorted_by_self = sorted(all_nodes, key=lambda n: n.self_ms, reverse=True)
            # Take nodes contributing >5% self-time, or top 10
            hot_nodes = [n for n in sorted_by_self if n.self_pct > 5]
            if len(hot_nodes) < 3:
                hot_nodes = sorted_by_self[:10]

            header = "### Problematic Nodes (by Exclusive Time)\n"

            md_parts.append(header)
            md_parts.append("| Node | Physical Op | Table.Index | Actual Rows | Rows Read | Self Time (ms) | Self % | Elapsed (ms) | Cost % | Logical Reads |")
            md_parts.append("|------|-----------|-------------|-------------|-----------|---------------|--------|-------------|--------|---------------|")
            for n in sorted(hot_nodes, key=lambda x: x.self_ms, reverse=True):
                tbl = n.table_name
                if n.index_name:
                    tbl += f".{n.index_name}"
                if n.is_lookup:
                    tbl += " (Lookup)"
                md_parts.append(
                    f"| {n.node_id} | {n.physical_op} | {tbl} | "
                    f"{n.actual_rows:,} | {n.actual_rows_read:,} | "
                    f"{n.self_ms:,} | {n.self_pct:.1f}% | "
                    f"{n.actual_elapsed_ms:,} | {n.cost_pct:.1f}% | {n.actual_logical_reads:,} |"
                )
            md_parts.append("")

            # DB-specific details for hot nodes
            md_parts.append("#### Details\n")
            for n in sorted(hot_nodes, key=lambda x: x.self_ms, reverse=True):
                md_parts.append(f"**Node {n.node_id} — {n.physical_op}** (Self: {n.self_ms:,}ms / {n.self_pct:.1f}%)")
                details = []
                if n.table_name:
                    details.append(f"- Table: `{n.table_name}`")
                if n.index_name:
                    details.append(f"- Index: `{n.index_name}`")
                if n.is_lookup:
                    details.append(f"- **Key Lookup** — consider covering index to avoid lookups")
                details.append(f"- Actual rows: {n.actual_rows:,} | Rows read: {n.actual_rows_read:,}")
                if n.actual_rows_read > 0 and n.actual_rows > 0:
                    selectivity = n.actual_rows / n.actual_rows_read * 100
                    if selectivity < 10:
                        details.append(f"- ⚠ Selectivity: {selectivity:.2f}% — reads {n.actual_rows_read:,} rows to produce {n.actual_rows:,}")
                details.append(f"- Executions: {n.actual_executions:,}")
                details.append(f"- Logical reads: {n.actual_logical_reads:,} | Physical reads: {n.actual_physical_reads:,}")
                details.append(f"- Elapsed: {n.actual_elapsed_ms:,}ms (self: {n.self_ms:,}ms) | CPU: {n.actual_cpu_ms:,}ms")
                md_parts.append("\n".join(details) + "\n")

            # Diagram with only problematic nodes
            if hot_nodes:
                hot_ids = {n.node_id for n in hot_nodes}
                hot_mermaid = _generate_mermaid_subset(plan_root, stmt_idx * 100, hot_ids)
                if hot_mermaid:
                    md_parts.append("### Problematic Nodes Diagram\n")
                    md_parts.append(f"```mermaid\n{hot_mermaid}\n```\n")

    return "\n".join(md_parts)


def main():
    if len(sys.argv) < 2:
        print("Usage: python sqlplan2mermaid.py <plan.xml>")
        sys.exit(1)

    xml_path = sys.argv[1]
    if not Path(xml_path).exists():
        print(f"File not found: {xml_path}")
        sys.exit(1)

    md_content = process_plan(xml_path)

    out_path = Path(xml_path).with_suffix(".md")
    out_path.write_text(md_content, encoding="utf-8")
    print(f"Output written to {out_path}")


if __name__ == "__main__":
    main()
