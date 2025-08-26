import React from 'react';
import { Box } from '@mui/material';

const AnimatedBackground: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <Box className="min-h-screen relative overflow-hidden fluid-gradient-bg">
      {/* Content */}
      <Box className="relative z-10">
        {children}
      </Box>
    </Box>
  );
};

export default AnimatedBackground;