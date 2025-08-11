import React from 'react';
import { Dashboard } from './components/Dashboard';
import { CssBaseline, Container } from '@mui/material';

/**
 * The main App component.
 * It sets up a baseline CSS and renders the main Dashboard.
 */
function App() {
  return (
    <React.Fragment>
      {/* CssBaseline is a component from Material-UI that provides a consistent baseline for styling. */}
      <CssBaseline />
      <Container maxWidth="lg">
        <header>
          {/* You can add a main application header or navigation bar here later. */}
          <h1>Trading Pipeline Dashboard</h1>
        </header>
        <main>
          {/* The main content of our application is the Dashboard. */}
          <Dashboard />
        </main>
      </Container>
    </React.Fragment>
  );
}

export default App;
