export interface SearchRequest {
  query: string;
  max_results?: number;
}

export interface QueryResult {
  query?: string;  // Made optional since backend doesn't always send it
  data: any;
  execution_time: number;
  row_count: number;
  interpretation?: string;
  display_format?: string;  // 'table' for SQL, 'neo4j_graph' for graph data
}

export interface SearchResponse {
  success: boolean;
  timestamp: string;
  query: string;
  keywords?: string[];  // Added to match backend response
  sql_results: QueryResult | null;
  graph_results: QueryResult | null;
  total_execution_time?: number;  // Added to match backend response
  ai_interpretation?: {
    sql_insights: string;
    graph_insights: string;
    comparison: string;
  };
  error?: string;
}

export interface StreamingTextProps {
  content: string;
  speed?: number;
  onComplete?: () => void;
  className?: string;
  displayFormat?: string;  // 'table' or 'neo4j_graph'
  rawData?: any;  // Raw data for graph formatting
}

export interface ResultsPanelProps {
  title: string;
  results: QueryResult | null;
  loading: boolean;
  error?: string;
}