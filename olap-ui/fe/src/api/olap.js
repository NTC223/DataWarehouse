/**
 * olap.js — API client for the OLAP Explorer backend.
 * All functions return Promises that resolve to JSON data.
 */

const BASE_URL = '/api';

/**
 * Generic fetch wrapper with error handling.
 */
async function request(url, options = {}) {
  const res = await fetch(`${BASE_URL}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

/**
 * List all available cubes with their metadata.
 */
export function fetchCubes() {
  return request('/cubes');
}

/**
 * Get dimensions and hierarchy for a specific cube.
 */
export function fetchCubeDimensions(cube) {
  return request(`/cubes/${cube}/dimensions`);
}

/**
 * Execute an OLAP query (drill-down, roll-up, slice, dice).
 * @param {string} cube - 'sales' or 'inventory'
 * @param {object} body - { dimensions, filters, sort_by, sort_order, limit, offset }
 */
export function queryOLAP(cube, body) {
  return request(`/cubes/${cube}/query`, {
    method: 'POST',
    body: JSON.stringify({ cube, ...body }),
  });
}

/**
 * Execute a drill-through query to get detail fact records.
 * @param {string} cube - 'sales' or 'inventory'
 * @param {object} body - { filters, limit, offset }
 */
export function drillThrough(cube, body) {
  return request(`/cubes/${cube}/drill-through`, {
    method: 'POST',
    body: JSON.stringify({ cube, ...body }),
  });
}

/**
 * Execute a drill-across query joining both cubes.
 * @param {object} body - { source_cube, dimensions, filters, sort_by, sort_order, limit, offset }
 */
export function drillAcross(body) {
  return request('/cubes/drill-across', {
    method: 'POST',
    body: JSON.stringify(body),
  });
}

/**
 * Fetch distinct values for a dimension column (with search + pagination).
 * @param {object} params - { cube, dimension, column, search, limit, offset }
 */
export function fetchDimValues(params) {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== null && v !== undefined && v !== '') qs.set(k, v);
  });
  return request(`/metadata/dim-values?${qs.toString()}`);
}

/**
 * Health check.
 */
export function healthCheck() {
  return request('/health');
}
