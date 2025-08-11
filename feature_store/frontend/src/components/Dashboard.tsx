import React from 'react';
import { useMarketData } from '../hooks/useMarketData';

// Import components from a UI library like Material-UI.
// NOTE: These dependencies (e.g., @mui/material) are not installed yet.
// The user will need to run 'npm install' to fetch them.
import { Box, Typography, Paper, CircularProgress } from '@mui/material';

/**
 * A simple dashboard component that displays the latest market data.
 */
export const Dashboard: React.FC = () => {
  // Use our custom hook to get the latest market data.
  const latestData = useMarketData();

  return (
    <Box sx={{ padding: 2 }}>
      <Typography variant="h4" gutterBottom>
        Real-Time Market Data
      </Typography>
      <Paper elevation={3} sx={{ padding: 2, marginTop: 2, minHeight: '100px' }}>
        {latestData ? (
          // If we have data, display it.
          <Box>
            <Typography variant="h6">
              Latest Tick for: {latestData.symbol}
            </Typography>
            <Typography variant="body1">
              Price: ${latestData.price}
            </Typography>
            {/* Add more fields here as they are added to the MarketData interface */}
          </Box>
        ) : (
          // If we don't have data yet, show a loading indicator.
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
            <CircularProgress />
            <Typography sx={{ marginLeft: 2 }}>
              Connecting to WebSocket and waiting for data...
            </Typography>
          </Box>
        )}
      </Paper>
    </Box>
  );
};
