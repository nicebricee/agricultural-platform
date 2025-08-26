import React, { useState } from 'react';
import { 
  Box, 
  TextField, 
  IconButton,
  InputAdornment,
  CircularProgress
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

interface SearchInterfaceProps {
  onSearch: (query: string) => void;
  loading: boolean;
}

const SearchInterface: React.FC<SearchInterfaceProps> = ({ onSearch, loading }) => {
  const [query, setQuery] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (query.trim() && !loading) {
      onSearch(query.trim()); // Always use streaming
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e as any);
    }
  };

  return (
    <Box 
      component="form" 
      onSubmit={handleSubmit}
      className="w-full max-w-2xl mx-auto px-4"
    >
      <TextField
        fullWidth
        variant="outlined"
        placeholder="Search..."
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyPress={handleKeyPress}
        disabled={loading}
        className="bg-white/95 backdrop-blur rounded-lg"
        InputProps={{
          className: "pr-2",
          endAdornment: (
            <InputAdornment position="end">
              <IconButton
                type="submit"
                disabled={loading || !query.trim()}
                className="bg-blue-600 text-white hover:bg-blue-700 disabled:bg-gray-400"
                size="small"
              >
                {loading ? (
                  <CircularProgress size={20} className="text-white" />
                ) : (
                  <SearchIcon />
                )}
              </IconButton>
            </InputAdornment>
          ),
        }}
        sx={{
          '& .MuiOutlinedInput-root': {
            '& fieldset': {
              borderColor: 'rgba(255, 255, 255, 0.3)',
            },
            '&:hover fieldset': {
              borderColor: 'rgba(255, 255, 255, 0.5)',
            },
            '&.Mui-focused fieldset': {
              borderColor: '#2196F3',
            },
          },
        }}
      />
    </Box>
  );
};

export default SearchInterface;