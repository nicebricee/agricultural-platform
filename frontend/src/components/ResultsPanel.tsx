import React from 'react';
import { 
  Paper, 
  Typography, 
  Box, 
  Skeleton,
  Alert 
} from '@mui/material';
import StreamingText from './StreamingText';
import { ResultsPanelProps } from '../types';
import { formatAsTable } from '../utils/tableFormatter';
import { formatAsGraphTable } from '../utils/graphTableFormatter';

const ResultsPanel: React.FC<ResultsPanelProps> = ({ 
  title, 
  results, 
  loading, 
  error
}) => {
  const getResultsContent = () => {
    let content = '';
    
    // First show AI interpretation if available
    if (results?.interpretation) {
      content += '=== ANALYSIS ===\n\n';
      content += results.interpretation;
      content += '\n\n';
    }
    
    // Then show the data table if available
    if (results?.data && Array.isArray(results.data) && results.data.length > 0) {
      // Check if this is graph data
      const isGraphData = results.display_format === 'neo4j_graph';
      content += isGraphData ? '=== GRAPH DATA ===\n\n' : '=== DATA TABLE ===\n\n';
      
      // Format the appropriate table type
      if (!isGraphData) {
        content += formatAsTable(results.data);
      } else {
        // Format graph data with Neo4j-style table
        content += formatAsGraphTable(results.data, results.display_format);
      }
    }
    
    // Fallback for other data types
    if (!content && results?.data) {
      if (typeof results.data === 'string') {
        content = results.data;
      } else {
        content = JSON.stringify(results.data, null, 2);
      }
    }
    
    return content || 'No results available';
  };

  return (
    <Paper 
      elevation={3}
      className="h-[500px] flex flex-col bg-white/95 backdrop-blur"
    >
      {/* Panel Header */}
      <Box className="px-6 py-4 border-b border-gray-200">
        <Typography 
          variant="h6" 
          className="font-semibold text-gray-800"
        >
          {title}
        </Typography>
        {results && (
          <Typography 
            variant="caption" 
            className="text-gray-500"
          >
            Query executed in {results.execution_time.toFixed(3)}s • 
            {results.row_count} results
          </Typography>
        )}
      </Box>

      {/* Panel Content */}
      <Box className="flex-1 px-6 py-4 overflow-y-auto custom-scrollbar">
        {loading && !results && (
          <Box>
            <Skeleton variant="text" width="80%" height={30} />
            <Skeleton variant="text" width="100%" height={30} />
            <Skeleton variant="text" width="90%" height={30} />
            <Skeleton variant="rectangular" width="100%" height={200} className="mt-4" />
          </Box>
        )}

        {error && (
          <Alert severity="error" className="mb-4">
            {error}
          </Alert>
        )}

        {/* Show results with typewriter effect */}
        {results && !loading && (
          <StreamingText 
            content={getResultsContent()}
            speed={20}
            displayFormat={results.display_format}
            rawData={results.data}
          />
        )}

        {!loading && !results && !error && (
          <Typography className="text-gray-400 italic">
            Enter a query to see results
          </Typography>
        )}
      </Box>
    </Paper>
  );
};

export default ResultsPanel;