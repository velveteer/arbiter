// Request and response types for an API route, read from the generated api.d.ts.
// Queue routes are typed through the one queue the spec is rendered with.
import type { components, paths } from './api';

type Op<P extends keyof paths, M extends keyof paths[P]> = NonNullable<paths[P][M]>;
type Json<R> = R extends { content: infer C } ? C[keyof C] : null;

export type Schema<N extends keyof components['schemas']> = components['schemas'][N];
export type Res<P extends keyof paths, M extends keyof paths[P] = 'get'> =
  Op<P, M> extends { responses: infer R } ? (200 extends keyof R ? Json<R[200]> : null) : never;
export type Query<P extends keyof paths, M extends keyof paths[P] = 'get'> =
  Op<P, M> extends { parameters: { query?: infer Q } } ? NonNullable<Q> : never;
export type Body<P extends keyof paths, M extends keyof paths[P]> =
  Op<P, M> extends { requestBody?: { content: infer C } } ? C[keyof C] : never;
