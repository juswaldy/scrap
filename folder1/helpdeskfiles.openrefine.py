result = 'Unknown'

firsttwo = {
  'ASCII Text': ['ASCII text,', 'news or', 'ASCII text', 'Non-ISO extended-ASCII'],
  'Unicode Text': ['UTF-8 Unicode', 'Little-endian UTF-16'],
  'ISO-8859 Text': ['ISO-8859 text,'],
  'Rich Text': ['Rich Text', 'EPUB document', 'LaTeX 2e', 'PostScript document'],
  'Image': ['PNG image', 'JPEG image', 'TIFF image', 'GIF image', 'gd-jpeg v1.0', 'PC bitmap,', 'LEAD Technologies', 'AppleMark, baseline,', 'File written', ', baseline,', '*, progressive,', 'Adobe Photoshop', '\\002, baseline,', 'GIMP XCF', 'JPEG 2000', 'LEADTOOLS v20.0,', 'Microsoft Office",', 'MS Windows', 'PolyView64 Version'], 'PDF': ['PDF document,'],
  'MS Excel': ['Microsoft Excel'],
  'MS Word': ['Microsoft Word'],
  'MS Access': ['Microsoft Access'],
  'MS Powerpoint': ['Microsoft PowerPoint'],
  'XML/HTML/SGML': ['HTML document,', 'XML 1.0', 'exported SGML', 'XML document,', 'Microsoft OOXML', 'SVG Scalable'],
  'Audio': ['MPEG ADTS,', 'Audio file', 'RIFF (little-endian)'],
  'Video': ['ISO Media,'],
  'Zip': ['Zip archive', 'Microsoft Cabinet'],
  'Attachments': ['CDFV2 Microsoft', 'Composite Document', 'vCard visiting', 'CDFV2 Encrypted', 'vCalendar calendar'],
  'Email': ['RFC 822', 'Microsoft Outlook'],
  'Binary': ['Apple binary', 'AppleDouble encoded', 'lif file', 'Win32'],
  'Executable': ['PE32 executable', 'PE32+ executable', 'Python script,'],
  'Signature/Certificate': ['PGP signature', 'RFC1421 Security'],
  'Unknown': ['0', '1', '4', '156', '518', '1104', '10000'],
}

for k, v in firsttwo.items():
  if value in v:
    result = k

return result
