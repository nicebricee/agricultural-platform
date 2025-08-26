import React, { useState, useEffect } from 'react';
import { 
  ThemeProvider, 
  createTheme, 
  CssBaseline,
  Box,
  Snackbar,
  Alert
} from '@mui/material';
import AnimatedBackground from './components/AnimatedBackground';
import Header from './components/Header';
import SearchInterface from './components/SearchInterface';
import ComparisonView from './components/ComparisonView';
import apiService from './services/api';
import { SearchResponse } from './types';

// Create MUI theme
const theme = createTheme({
  palette: {
    primary: {
      main: '#2196F3',
    },
    secondary: {
      main: '#4CAF50',
    },
  },
  typography: {
    fontFamily: [
      '-apple-system',
      'BlinkMacSystemFont',
      '"Segoe UI"',
      'Roboto',
      '"Helvetica Neue"',
      'Arial',
      'sans-serif',
    ].join(','),
  },
});

function App() {
  const [searchResponse, setSearchResponse] = useState<SearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [backendHealthy, setBackendHealthy] = useState(true);

  // Check backend health on mount and periodically
  useEffect(() => {
    let retryCount = 0;
    const MAX_RETRIES = 3;
    
    const checkHealth = async () => {
      try {
        const isHealthy = await apiService.checkHealth();
        if (isHealthy) {
          retryCount = 0; // Reset retry count on success
          setBackendHealthy(true);
        } else {
          retryCount++;
          if (retryCount >= MAX_RETRIES) {
            setBackendHealthy(false);
            setError('Backend server is not responding. Please ensure it is running on http://localhost:8000');
          }
        }
      } catch (error) {
        retryCount++;
        if (retryCount >= MAX_RETRIES) {
          setBackendHealthy(false);
          setError('Backend server is not responding. Please ensure it is running on http://localhost:8000');
        }
      }
    };
    
    // Initial check
    checkHealth();
    
    // Check every 30 seconds
    const interval = setInterval(checkHealth, 30000);
    
    return () => clearInterval(interval);
  }, []);


  const handleSearch = async (query: string) => {
    console.log('Starting search for:', query);
    setLoading(true);
    setError(null);
    setSearchResponse(null);
    
    try {
      const response = await apiService.search({
        query,
        max_results: 50,
      });
      
      console.log('Got response:', response);
      console.log('Response has sql_results?', !!response.sql_results);
      console.log('Response has graph_results?', !!response.graph_results);
      
      setSearchResponse(response);
      setLoading(false);
      console.log('Loading set to false');
    } catch (err: any) {
      console.error('Search error:', err);
      setError(err.message || 'An unexpected error occurred');
      setLoading(false);
    }
  };

  const handleCloseError = () => {
    setError(null);
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <AnimatedBackground>
        <Box className="min-h-screen flex flex-col">
          {/* Header */}
          <Header />

          {/* Main Content */}
          <Box className="flex-1 flex flex-col justify-center">
            {/* Results Panels */}
            <ComparisonView 
              searchResponse={searchResponse} 
              loading={loading}
            />

            {/* Search Bar */}
            <Box className="py-6">
              <SearchInterface 
                onSearch={handleSearch} 
                loading={loading} 
              />
            </Box>
          </Box>

          {/* Error Snackbar */}
          <Snackbar
            open={!!error}
            autoHideDuration={6000}
            onClose={handleCloseError}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
          >
            <Alert 
              onClose={handleCloseError} 
              severity="error" 
              sx={{ width: '100%' }}
            >
              {error}
            </Alert>
          </Snackbar>

          {/* Backend Health Warning */}
          {!backendHealthy && (
            <Snackbar
              open={true}
              anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
            >
              <Alert severity="warning" sx={{ width: '100%' }}>
                Backend not connected. Start the server with: cd backend && python -m uvicorn main:app --reload
              </Alert>
            </Snackbar>
          )}
        </Box>
      </AnimatedBackground>
    </ThemeProvider>
  );
}

export default App;