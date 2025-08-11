import { useState, useEffect } from 'react';
import { io, Socket } from 'socket.io-client';

// Define the shape of the market data we expect to receive.
// This should match the structure of the data sent by the FastAPI backend.
interface MarketData {
  symbol: string;
  price: string;
  // Add other fields from your data stream here, e.g., volume, timestamp
}

// Define the WebSocket URL.
// In a real application, this should come from an environment variable.
// The '/api' path will be proxied by our Nginx server in production to the api_backend service.
const WEBSOCKET_URL = 'ws://localhost:8000/ws/market-data'; // For local dev, this would point directly to the backend.

/**
 * A custom hook to connect to the market data WebSocket stream.
 * It manages the WebSocket connection and provides the latest received message.
 *
 * @returns The latest market data message received from the WebSocket, or null if no message has been received yet.
 */
export const useMarketData = () => {
  // State to hold the latest market data message.
  const [latestData, setLatestData] = useState<MarketData | null>(null);

  useEffect(() => {
    // --- WebSocket Connection Setup ---
    // Create a new Socket.io client instance.
    // We are using socket.io-client here, but a standard WebSocket client would also work.
    // The path option is important for the Nginx proxy to work correctly.
    const socket: Socket = io(WEBSOCKET_URL, {
      transports: ['websocket'], // Force WebSocket transport
    });

    // --- Event Listeners ---
    // Listener for the 'connect' event.
    socket.on('connect', () => {
      console.log('WebSocket connected successfully.');
    });

    // Listener for incoming messages. The event name 'message' or 'market_data'
    // should match what the backend is broadcasting. Let's assume a generic 'message' event.
    socket.on('message', (message: string) => {
      try {
        // The backend sends data as a JSON string, so we need to parse it.
        const data: MarketData = JSON.parse(message);
        setLatestData(data);
      } catch (error) {
        console.error('Failed to parse incoming WebSocket message:', error);
      }
    });

    // Listener for the 'disconnect' event.
    socket.on('disconnect', (reason) => {
      console.log(`WebSocket disconnected: ${reason}`);
    });

    // Listener for connection errors.
    socket.on('connect_error', (error) => {
      console.error('WebSocket connection error:', error);
    });


    // --- Cleanup Logic ---
    // The returned function from useEffect is a cleanup function.
    // It will be called when the component that uses this hook unmounts.
    return () => {
      console.log('Disconnecting WebSocket.');
      socket.disconnect();
    };
  }, []); // The empty dependency array [] means this effect runs only once when the component mounts.

  // Return the latest data for the component to use.
  return latestData;
};
