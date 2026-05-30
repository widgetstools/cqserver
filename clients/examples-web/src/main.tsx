import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { bootstrapAtlasTheme } from '@/atlas/theme/ThemeContext';
import '@/atlas/tokens.css';
import './styles/globals.css';
import { App } from './App';

bootstrapAtlasTheme();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
