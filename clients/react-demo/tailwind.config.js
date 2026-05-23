/** @type {import('tailwindcss').Config} */
export default {
  darkMode: ['selector', '[data-theme="dark"]'],
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Shadcn HSL tokens (defined in palettes.css)
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))',
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))',
        },
        muted: {
          DEFAULT: 'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))',
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))',
        },
        destructive: {
          DEFAULT: 'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))',
        },
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',

        // Stockflux SF tokens — useful for charts / one-offs
        sf: {
          bg: 'var(--sf-bg)',
          'bg-2': 'var(--sf-bg-2)',
          'bg-3': 'var(--sf-bg-3)',
          'bg-4': 'var(--sf-bg-4)',
          border: 'var(--sf-border)',
          'border-2': 'var(--sf-border-2)',
          t0: 'var(--sf-t-0)',
          t1: 'var(--sf-t-1)',
          t2: 'var(--sf-t-2)',
          t3: 'var(--sf-t-3)',
          up: 'var(--sf-up)',
          down: 'var(--sf-down)',
          flat: 'var(--sf-flat)',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'monospace'],
      },
      borderRadius: {
        lg: 'var(--radius, 0.5rem)',
        md: 'calc(var(--radius, 0.5rem) - 2px)',
        sm: 'calc(var(--radius, 0.5rem) - 4px)',
      },
    },
  },
  plugins: [],
};
