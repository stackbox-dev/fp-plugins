/**
 * Run cleanup functions sequentially, collecting errors without stopping.
 * Sequential (not parallel) because close operations often have ordering
 * dependencies (e.g., close sender before client). Every function runs
 * even if earlier ones fail.
 */
export async function safeCloseAll(
  ...fns: (() => Promise<void> | void)[]
): Promise<unknown[]> {
  const errors: unknown[] = [];
  for (const fn of fns) {
    try {
      await fn();
    } catch (err) {
      errors.push(err);
    }
  }
  return errors;
}
