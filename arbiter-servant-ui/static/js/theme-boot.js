// Applies the stored theme before first paint, so a reload never flashes the other one.
// Loaded synchronously from <head>, ahead of the stylesheets.
document.documentElement.setAttribute('data-bs-theme',
  localStorage.getItem('arbiter-theme') || 'dark');
