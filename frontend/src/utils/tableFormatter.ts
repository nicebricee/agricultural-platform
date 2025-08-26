/**
 * Clean ASCII Table formatter with Claude-style formatting
 */

// Box-drawing characters for clean tables
const BOX = {
  topLeft: '┌',
  topRight: '┐',
  bottomLeft: '└',
  bottomRight: '┘',
  horizontal: '─',
  vertical: '│',
  cross: '┼',
  topJoin: '┬',
  bottomJoin: '┴',
  leftJoin: '├',
  rightJoin: '┤',
  doubleHorizontal: '═',
  headerLeftJoin: '╞',
  headerRightJoin: '╡',
  headerCross: '╪'
};

/**
 * Format a number with thousands separators
 */
const formatNumber = (value: any): string => {
  if (typeof value === 'number') {
    // Check if it's a decimal
    if (value % 1 !== 0) {
      return value.toLocaleString('en-US', { 
        minimumFractionDigits: 2, 
        maximumFractionDigits: 2 
      });
    }
    // Integer
    return value.toLocaleString('en-US');
  }
  return String(value);
};

/**
 * Determine if a value should be right-aligned (numbers)
 */
const isNumeric = (value: any): boolean => {
  return typeof value === 'number' || !isNaN(parseFloat(value));
};

/**
 * Pad string with alignment
 */
const padCell = (value: string, width: number, alignRight: boolean = false): string => {
  if (alignRight) {
    return value.padStart(width, ' ');
  }
  return value.padEnd(width, ' ');
};

export const formatAsTable = (data: any[]): string => {
  if (!data || data.length === 0) {
    return 'No data available';
  }

  // Get headers from first row
  const headers = Object.keys(data[0]);
  
  // Calculate column widths and alignment
  const colWidths: Record<string, number> = {};
  const colAlignments: Record<string, boolean> = {};
  
  headers.forEach(header => {
    // Start with header length
    colWidths[header] = header.length;
    
    // Check if column contains numbers for alignment
    let hasNumbers = true;
    
    data.forEach(row => {
      const rawValue = row[header];
      const value = formatNumber(rawValue);
      const strValue = String(value ?? '');
      
      // Update max width (cap at 40 for long content)
      colWidths[header] = Math.min(Math.max(colWidths[header], strValue.length), 40);
      
      // Check if all values in column are numeric
      if (hasNumbers && rawValue !== null && rawValue !== undefined) {
        hasNumbers = hasNumbers && isNumeric(rawValue);
      }
    });
    
    colAlignments[header] = hasNumbers;
  });

  // Build top border
  let topBorder = BOX.topLeft;
  headers.forEach((header, idx) => {
    topBorder += BOX.horizontal.repeat(colWidths[header] + 2);
    topBorder += idx < headers.length - 1 ? BOX.topJoin : BOX.topRight;
  });

  // Build header row
  let headerRow = BOX.vertical;
  headers.forEach(header => {
    const padded = padCell(header, colWidths[header], false); // Headers always left-aligned
    headerRow += ` ${padded} ${BOX.vertical}`;
  });

  // Build header separator (with double line for emphasis)
  let headerSeparator = BOX.headerLeftJoin;
  headers.forEach((header, idx) => {
    headerSeparator += BOX.doubleHorizontal.repeat(colWidths[header] + 2);
    headerSeparator += idx < headers.length - 1 ? BOX.headerCross : BOX.headerRightJoin;
  });

  // Build data rows
  const dataRows: string[] = [];
  data.slice(0, 100).forEach(row => { // Limit to 100 rows for performance
    let dataRow = BOX.vertical;
    headers.forEach(header => {
      const rawValue = row[header];
      let value = formatNumber(rawValue);
      
      // Handle null/undefined
      if (rawValue === null || rawValue === undefined) {
        value = '';
      }
      
      // Truncate if too long
      if (value.length > 40) {
        value = value.substring(0, 37) + '...';
      }
      
      const padded = padCell(value, colWidths[header], colAlignments[header]);
      dataRow += ` ${padded} ${BOX.vertical}`;
    });
    dataRows.push(dataRow);
  });

  // Build bottom border
  let bottomBorder = BOX.bottomLeft;
  headers.forEach((header, idx) => {
    bottomBorder += BOX.horizontal.repeat(colWidths[header] + 2);
    bottomBorder += idx < headers.length - 1 ? BOX.bottomJoin : BOX.bottomRight;
  });

  // Assemble table
  const tableLines = [
    topBorder,
    headerRow,
    headerSeparator,
    ...dataRows,
    bottomBorder
  ];

  // Add row count (centered)
  const tableWidth = topBorder.length;
  const rowCountText = data.length > 100 
    ? `${data.length} rows (showing first 100)` 
    : `${data.length} row${data.length === 1 ? '' : 's'}`;
  const padding = Math.max(0, Math.floor((tableWidth - rowCountText.length) / 2));
  const centeredRowCount = ' '.repeat(padding) + rowCountText;
  
  tableLines.push(centeredRowCount);

  return tableLines.join('\n');
};