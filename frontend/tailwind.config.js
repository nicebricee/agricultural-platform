/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'forest-dark': '#0d3b0d',
        'forest-mid': '#1a5c1a',
        'forest': '#134e13',
        'forest-light': '#2d7a2d',
        'header-dark': '#2B3244',
      },
      animation: {
        'gradient': 'gradient 15s ease infinite',
        'liquid': 'liquid 20s ease infinite',
      },
      keyframes: {
        gradient: {
          '0%, 100%': {
            'background-position': '0% 50%',
          },
          '50%': {
            'background-position': '100% 50%',
          },
        },
        liquid: {
          '0%': {
            'background-position': '0% 50%',
            'filter': 'hue-rotate(0deg)',
          },
          '50%': {
            'background-position': '100% 50%',
            'filter': 'hue-rotate(10deg)',
          },
          '100%': {
            'background-position': '0% 50%',
            'filter': 'hue-rotate(0deg)',
          },
        },
      },
      backgroundSize: {
        '400%': '400% 400%',
      },
    },
  },
  plugins: [],
  important: true,
}