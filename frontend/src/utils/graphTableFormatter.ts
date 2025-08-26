/**
 * Graph Table Formatter
 * Formats Neo4j graph results with nodes, labels, and relationships
 * using ASCII box-drawing characters in Neo4j style
 */

import { formatAsTable } from './tableFormatter';

interface GraphNode {
  node_id: string;
  labels: string;
  name: string;
  properties: string[];
  relationships: string[];
}

interface GraphData {
  display_format?: string;
  data: GraphNode[] | any[];
}

// Neo4j-style box drawing characters
const NEO4J_BOX = {
  // Double lines for header
  topLeft: '╔',
  topRight: '╗',
  bottomLeft: '╚',
  bottomRight: '╝',
  doubleHorizontal: '═',
  doubleVertical: '║',
  doubleTopJoin: '╤',
  doubleBottomJoin: '╧',
  doubleLeftJoin: '╟',
  doubleRightJoin: '╢',
  doubleCross: '╪',
  
  // Single lines for data rows
  horizontal: '─',
  vertical: '│',
  leftJoin: '├',
  rightJoin: '┤',
  topJoin: '┬',
  bottomJoin: '┴',
  cross: '┼',
};

/**
 * Format graph data as a Neo4j-style ASCII table
 */
export function formatAsGraphTable(data: any, display_format?: string): string {
  // Check if this is graph format data
  if (display_format !== 'neo4j_graph' || !Array.isArray(data) || data.length === 0) {
    // Fallback to regular table formatting
    return formatAsRegularTable(data);
  }

  // Cast to GraphNode array
  const graphNodes = data as GraphNode[];
  
  // Define columns (removed Node ID as it's not user-friendly)
  const columns = [
    { key: 'labels', header: 'Labels', width: 15 },
    { key: 'name', header: 'Name', width: 25 },
    { key: 'properties', header: 'Properties', width: 35 },
    { key: 'relationships', header: 'Relationships', width: 30 }
  ];

  // Calculate actual column widths based on content
  columns.forEach(col => {
    col.width = Math.max(
      col.header.length,
      ...graphNodes.map(node => {
        const value = getNodeValue(node, col.key);
        return value.length;
      })
    ) + 2; // Add padding
  });

  // Build the table
  let table = '';

  // Top border with double lines
  table += NEO4J_BOX.topLeft;
  columns.forEach((col, i) => {
    table += NEO4J_BOX.doubleHorizontal.repeat(col.width);
    if (i < columns.length - 1) {
      table += NEO4J_BOX.doubleTopJoin;
    }
  });
  table += NEO4J_BOX.topRight + '\n';

  // Header row
  table += NEO4J_BOX.doubleVertical;
  columns.forEach(col => {
    table += ' ' + padString(col.header, col.width - 2) + ' ';
    table += NEO4J_BOX.vertical;
  });
  table = table.slice(0, -1) + NEO4J_BOX.doubleVertical + '\n';

  // Header separator with double lines transitioning to single
  table += NEO4J_BOX.doubleLeftJoin;
  columns.forEach((col, i) => {
    table += NEO4J_BOX.doubleHorizontal.repeat(col.width);
    if (i < columns.length - 1) {
      table += NEO4J_BOX.doubleCross;
    }
  });
  table += NEO4J_BOX.doubleRightJoin + '\n';

  // Data rows
  graphNodes.forEach((node, rowIndex) => {
    // For multi-line cells (properties and relationships)
    const maxLines = Math.max(
      node.properties?.length || 1,
      node.relationships?.length || 1
    );

    for (let lineIndex = 0; lineIndex < maxLines; lineIndex++) {
      table += NEO4J_BOX.doubleVertical;
      
      columns.forEach(col => {
        let value = '';
        
        if (lineIndex === 0 && (col.key === 'node_id' || col.key === 'labels' || col.key === 'name')) {
          // Single-line values only on first line
          value = getNodeValue(node, col.key);
        } else if (col.key === 'properties' && node.properties) {
          value = node.properties[lineIndex] || '';
        } else if (col.key === 'relationships' && node.relationships) {
          value = node.relationships[lineIndex] || '';
        }
        
        table += ' ' + padString(value, col.width - 2) + ' ';
        table += NEO4J_BOX.vertical;
      });
      
      table = table.slice(0, -1) + NEO4J_BOX.doubleVertical + '\n';
    }
    
    // Row separator (except for last row)
    if (rowIndex < graphNodes.length - 1) {
      table += NEO4J_BOX.leftJoin;
      columns.forEach((col, i) => {
        table += NEO4J_BOX.horizontal.repeat(col.width);
        if (i < columns.length - 1) {
          table += NEO4J_BOX.cross;
        }
      });
      table += NEO4J_BOX.rightJoin + '\n';
    }
  });

  // Bottom border with double lines
  table += NEO4J_BOX.bottomLeft;
  columns.forEach((col, i) => {
    table += NEO4J_BOX.doubleHorizontal.repeat(col.width);
    if (i < columns.length - 1) {
      table += NEO4J_BOX.doubleBottomJoin;
    }
  });
  table += NEO4J_BOX.bottomRight + '\n';

  // Add row count
  table += `\n(${graphNodes.length} nodes)\n`;

  return table;
}

/**
 * Get value from a graph node by key
 */
function getNodeValue(node: GraphNode, key: string): string {
  switch (key) {
    case 'node_id':
      return node.node_id || '';
    case 'labels':
      return node.labels || '[:Node]';
    case 'name':
      return node.name || '';
    case 'properties':
      return node.properties && node.properties.length > 0 ? node.properties[0] : '';
    case 'relationships':
      return node.relationships && node.relationships.length > 0 ? node.relationships[0] : '';
    default:
      return '';
  }
}

/**
 * Fallback to regular table formatting for non-graph data
 */
function formatAsRegularTable(data: any[]): string {
  if (!Array.isArray(data) || data.length === 0) {
    return 'No data available';
  }

  // Use the regular table formatter
  return formatAsTable(data);
}

/**
 * Pad or truncate string to specified width
 */
function padString(str: string, width: number): string {
  if (!str) str = '';
  if (str.length > width) {
    return str.substring(0, width - 1) + '…';
  }
  return str.padEnd(width);
}

/**
 * Format a number with thousand separators
 */
function formatNumber(num: any): string {
  if (typeof num === 'number') {
    return num.toLocaleString();
  }
  return String(num);
}