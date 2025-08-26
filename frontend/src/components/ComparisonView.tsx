import React from 'react';
import { 
  Box, 
  Container,
  Grid
} from '@mui/material';
import ResultsPanel from './ResultsPanel';
import StreamingText from './StreamingText';
import { SearchResponse } from '../types';
import { useResponsive } from '../hooks/useResponsive';

interface ComparisonViewProps {
  searchResponse: SearchResponse | null;
  loading: boolean;
}

const ComparisonView: React.FC<ComparisonViewProps> = ({ 
  searchResponse, 
  loading
}) => {
  const { shouldStackPanels } = useResponsive();

  // Extract interpreted results if available
  const sqlResults = searchResponse?.sql_results ? {
    ...searchResponse.sql_results,
    interpretation: searchResponse.ai_interpretation?.sql_insights || searchResponse.sql_results.interpretation
  } : null;

  const graphResults = searchResponse?.graph_results ? {
    ...searchResponse.graph_results,
    interpretation: searchResponse.ai_interpretation?.graph_insights || searchResponse.graph_results.interpretation
  } : null;

  return (
    <Container maxWidth={false} className="py-8 px-4">
      <Grid 
        container 
        spacing={2}
      >
        {/* Traditional Dataset Panel */}
        <Grid size={{ xs: 12, lg: 6 }}>
          <ResultsPanel
            title="Traditional Dataset"
            results={sqlResults}
            loading={loading}
            error={searchResponse?.error && !sqlResults ? searchResponse.error : undefined}
          />
        </Grid>

        {/* Knowledge Graph Dataset Panel */}
        <Grid size={{ xs: 12, lg: 6 }}>
          <ResultsPanel
            title="Knowledge Graph Dataset"
            results={graphResults}
            loading={loading}
            error={searchResponse?.error && !graphResults ? searchResponse.error : undefined}
          />
        </Grid>
      </Grid>

      {/* Comparison Summary (if available) */}
      {searchResponse?.ai_interpretation?.comparison && (
        <Box className="mt-6 p-4 bg-white/90 rounded-lg">
          <StreamingText
            content={searchResponse.ai_interpretation.comparison}
            speed={25}
          />
        </Box>
      )}
    </Container>
  );
};

export default ComparisonView;