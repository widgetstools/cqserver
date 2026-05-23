import { createRoot } from 'react-dom/client';
import App from './App';
import './styles/tailwind.css';

// StrictMode is intentionally disabled — the dev double-mount creates a
// ghost WebSocket subscription that the cleanup races to tear down. The
// ghost can leak ~3k delta drops/sec server-side until it's finally
// closed. For a streaming demo the double-render isn't worth the noise.
const root = createRoot(document.getElementById('root')!);
root.render(<App />);
