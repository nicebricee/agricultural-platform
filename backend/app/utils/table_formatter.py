"""
ASCII Table formatter for database results.
"""

from typing import List, Dict, Any
import json


def format_as_ascii_table(data: List[Dict[str, Any]], max_width: int = 20, max_rows: int = 200) -> str:
    """
    Format data as ASCII table with borders.
    
    Args:
        data: List of dictionaries to format
        max_width: Maximum width for each column
        max_rows: Maximum number of rows to display
        
    Returns:
        ASCII formatted table string
    """
    if not data:
        return "No data available"
    
    # Get headers from first row
    headers = list(data[0].keys())
    
    # Limit data rows
    display_data = data[:max_rows]
    truncated = len(data) > max_rows
    
    # Calculate column widths
    col_widths = {}
    for header in headers:
        # Start with header length
        col_widths[header] = min(len(str(header)), max_width)
        # Check all data rows
        for row in display_data:
            value = str(row.get(header, ''))
            if len(value) > max_width:
                value = value[:max_width-3] + '...'
            col_widths[header] = max(col_widths[header], len(value))
    
    # Build separator line
    separator = '+'
    for header in headers:
        separator += '-' * (col_widths[header] + 2) + '+'
    
    # Build header row
    header_row = '|'
    for header in headers:
        header_str = str(header)[:col_widths[header]]
        header_row += f' {header_str:<{col_widths[header]}} |'
    
    # Build data rows
    data_rows = []
    for row in display_data:
        data_row = '|'
        for header in headers:
            value = str(row.get(header, ''))
            if len(value) > max_width:
                value = value[:max_width-3] + '...'
            data_row += f' {value:<{col_widths[header]}} |'
        data_rows.append(data_row)
    
    # Assemble table
    table_lines = [
        separator,
        header_row,
        separator
    ]
    table_lines.extend(data_rows)
    table_lines.append(separator)
    
    if truncated:
        table_lines.append(f"... {len(data) - max_rows} more rows ...")
    
    table_lines.append(f"Total: {len(data)} rows")
    
    return '\n'.join(table_lines)


def format_results_with_tables(
    sql_results: List[Dict[str, Any]] = None,
    graph_results: List[Dict[str, Any]] = None
) -> Dict[str, str]:
    """
    Format both SQL and Graph results as ASCII tables.
    
    Returns:
        Dictionary with 'sql_table' and 'graph_table' keys
    """
    result = {}
    
    if sql_results:
        result['sql_table'] = format_as_ascii_table(sql_results)
    else:
        result['sql_table'] = "No SQL results available"
    
    if graph_results:
        result['graph_table'] = format_as_ascii_table(graph_results)
    else:
        result['graph_table'] = "No Graph results available"
    
    return result