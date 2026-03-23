"""
Script to inspect HTML sections in BZP notices stored as Parquet files.
"""
import sys
import os
import re
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path

BRONZE_ROOT = Path("E:/git_projects/procurement-watchdog-api-exploration/data/bronze/notices")

def find_notice(notice_type: str, object_id: str):
    """Scan all date partitions for a notice type to find the given objectId."""
    base = BRONZE_ROOT / f"noticeType={notice_type}"
    if not base.exists():
        print(f"  ERROR: {base} does not exist")
        return None, None
    for date_dir in sorted(base.iterdir()):
        date_str = date_dir.name.replace("publicationDateDay=", "")
        for parquet_file in date_dir.glob("part-*.parquet"):
            table = pq.read_table(parquet_file, columns=["objectId", "htmlBody"])
            ids = table.column("objectId").to_pylist()
            if object_id in ids:
                idx = ids.index(object_id)
                html = table.column("htmlBody").to_pylist()[idx]
                return date_str, html
    return None, None


def extract_section_context(html: str, section_number: str, context_chars: int = 300):
    """
    Find all h3 tags containing the section number and extract surrounding context.
    Returns list of (header_text, value_text, raw_context).
    """
    if html is None:
        return []

    results = []
    # Look for h3 tags containing this section number (with word boundary awareness)
    # Section number like "8.8", "1.10", "4.2.3" etc.
    # Escape dots for regex
    escaped = re.escape(section_number)
    # Match h3 tag that contains the section number (possibly with trailing period or space)
    pattern = re.compile(
        r'(<h3[^>]*>.*?' + escaped + r'[^<]*</h3>)',
        re.IGNORECASE | re.DOTALL
    )

    for match in pattern.finditer(html):
        h3_text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
        start = match.start()
        end = match.end()
        # Get context after the h3 tag
        after = html[end:end + context_chars * 3]
        # Get context before
        before = html[max(0, start - context_chars):start]

        # Extract the next meaningful content after the h3 (skip whitespace/empty tags)
        # Find text content in the next ~500 chars
        after_text = re.sub(r'<[^>]+>', ' ', after[:500]).strip()
        after_text = re.sub(r'\s+', ' ', after_text)[:400]

        before_text = re.sub(r'<[^>]+>', ' ', before[-200:]).strip()
        before_text = re.sub(r'\s+', ' ', before_text)

        results.append({
            'h3_text': h3_text,
            'after_text': after_text,
            'before_text': before_text,
            'raw_after': after[:500],
        })

    return results


def print_task(task_num: str, notice_type: str, object_id: str, sections: list, extra_note: str = ""):
    print("=" * 80)
    print(f"TASK {task_num} — {notice_type}, sections: {', '.join(sections)}")
    if extra_note:
        print(f"  Note: {extra_note}")
    print(f"  objectId: {object_id}")

    date, html = find_notice(notice_type, object_id)
    if html is None:
        print(f"  ERROR: objectId not found in noticeType={notice_type}")
        return

    print(f"  Found in publicationDateDay={date}")
    print(f"  HTML length: {len(html)} chars")
    print()

    for sec in sections:
        results = extract_section_context(html, sec)
        if not results:
            print(f"  --- Section {sec}: NOT FOUND in HTML ---")
            # Try a looser search
            idx = html.find(sec)
            if idx >= 0:
                snippet = html[max(0,idx-100):idx+200]
                snippet_text = re.sub(r'<[^>]+>', ' ', snippet).strip()
                snippet_text = re.sub(r'\s+', ' ', snippet_text)
                print(f"  (raw occurrence context): {snippet_text}")
        else:
            for i, r in enumerate(results):
                print(f"  --- Section {sec} occurrence #{i+1} ---")
                print(f"  H3 header: {r['h3_text']}")
                print(f"  Content after: {r['after_text']}")
                if r['before_text']:
                    print(f"  Context before: ...{r['before_text'][-150:]}")
                print()


# ============================================================
# TASK 1: ContractNotice, section 8.8
# ============================================================
print_task("1", "ContractNotice", "08dd3af7-c0f0-cebb-8f1b-b600017b45b9", ["8.8"])

# ============================================================
# TASK 2: ContractNotice, section 1.10
# ============================================================
print_task("2", "ContractNotice", "08dd8235-e543-10b9-eaaa-350001a39742", ["1.10"])

# ============================================================
# TASK 3: ContractNotice, section 8.23
# ============================================================
print_task("3", "ContractNotice", "08dd7ce7-d094-12ca-eaaa-350001a36de5", ["8.23"])

# ============================================================
# TASK 4: ContractNotice, section 4.2.3
# ============================================================
print_task("4", "ContractNotice", "08ddce89-333d-d611-e769-360001c6cf64", ["4.2.3"])

# ============================================================
# TASK 5: TenderResultNotice, section 4.5.2
# ============================================================
print_task("5", "TenderResultNotice", "08dd6536-b043-6194-8f1b-b600017d5edd", ["4.5.2"])

# ============================================================
# TASK 6: ContractNotice, parse_tak_nie sections, value '0'
# ============================================================
print("=" * 80)
print("TASK 6 — ContractNotice, parse_tak_nie sections showing '0'")
object_id_6 = "08dd45c4-4a80-be3c-8f1b-b600017bf18e"
tak_nie_sections = ["4.2.4", "4.2.8", "4.2.11", "4.2.13", "4.3.10"]
print(f"  objectId: {object_id_6}")

date6, html6 = find_notice("ContractNotice", object_id_6)
if html6 is None:
    print("  ERROR: not found")
else:
    print(f"  Found in publicationDateDay={date6}")
    print(f"  HTML length: {len(html6)} chars")
    print()
    for sec in tak_nie_sections:
        results = extract_section_context(html6, sec)
        if not results:
            print(f"  --- Section {sec}: NOT FOUND ---")
        else:
            for i, r in enumerate(results):
                print(f"  --- Section {sec} occurrence #{i+1} ---")
                print(f"  H3 header: {r['h3_text']}")
                print(f"  Content: {r['after_text'][:200]}")
                # Check for '0' in content
                if '0' in r['after_text'][:50]:
                    print(f"  *** CONTAINS '0' near start ***")
                print()

# ============================================================
# TASK 7: ContractNotice, parse_tak_nie returns part title
# ============================================================
print("=" * 80)
print("TASK 7 — ContractNotice, parse_tak_nie sections returning part title")
object_id_7 = "08dd728b-d581-3075-8f1b-b600017e041c"
print(f"  objectId: {object_id_7}")

date7, html7 = find_notice("ContractNotice", object_id_7)
if html7 is None:
    print("  ERROR: not found")
else:
    print(f"  Found in publicationDateDay={date7}")
    print(f"  HTML length: {len(html7)} chars")
    print()
    # For task 7, show ALL occurrences of those sections across parts
    for sec in tak_nie_sections:
        results = extract_section_context(html7, sec)
        if not results:
            print(f"  --- Section {sec}: NOT FOUND ---")
        else:
            # Show first 4 occurrences
            print(f"  --- Section {sec}: {len(results)} occurrence(s) ---")
            for i, r in enumerate(results[:5]):
                print(f"    Occurrence #{i+1}:")
                print(f"    H3: {r['h3_text']}")
                print(f"    Content: {r['after_text'][:250]}")
                print()

print("=" * 80)
print("DONE")
