// Applies the stored theme before first paint, so a reload never flashes the other one.
// Loaded synchronously from <head>, ahead of the stylesheets.
{
  let theme = null;
  try {
    theme = localStorage.getItem('arb.theme');
  } catch {
    // Site data is blocked.
  }
  document.documentElement.setAttribute('data-bs-theme', theme || 'dark');
}
