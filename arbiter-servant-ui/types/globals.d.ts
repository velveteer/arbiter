// Globals the dashboard reads from classic scripts.
declare const cronstrue: { toString(expr: string): string };

// TypeScript's lib does not declare JSON.rawJSON.
interface JSON {
  rawJSON(text: string): unknown;
}
