import React from 'react';
import { Box, Typography } from '@mui/material';

const Header: React.FC = () => {
  return (
    <Box className="py-12 px-4">
      <Typography 
        variant="h3" 
        component="h1"
        className="text-white text-center font-bold mb-3 drop-shadow-lg"
        sx={{
          textShadow: '2px 2px 4px rgba(0,0,0,0.3)'
        }}
      >
        Traditional vs Knowledge Graph Data
      </Typography>
      <Typography 
        variant="h6" 
        component="h2"
        className="text-white/90 text-center font-normal"
        sx={{
          textShadow: '1px 1px 2px rgba(0,0,0,0.2)'
        }}
      >
        What do you wish to know about the agriculture sector?
      </Typography>
    </Box>
  );
};

export default Header;