// @ts-check
const js = require("@eslint/js");
const tseslint = require("typescript-eslint");
const n = require("eslint-plugin-n");

module.exports = tseslint.config(
  {
    ignores: [
      "dist/**",
      "coverage/**",
      "node_modules/**",
      // Standalone example app, not part of the tsconfig program.
      "sample/**",
    ],
  },

  // Root config files are plain CommonJS and outside tsconfig's "src" include, so the
  // type-aware rules have no program for them.
  {
    files: ["*.js"],
    ...js.configs.recommended,
    languageOptions: {
      sourceType: "commonjs",
      globals: {
        module: "writable",
        require: "readonly",
        __dirname: "readonly",
      },
    },
  },

  // Everything below is type-aware and scoped to the sources.
  {
    files: ["src/**/*.ts"],
    extends: [
      js.configs.recommended,
      ...tseslint.configs.recommendedTypeChecked,
    ],
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: __dirname },
    },
    plugins: { n },
    rules: {
      // Unhandled rejections in a storage client surface as silent data loss.
      "@typescript-eslint/no-floating-promises": "error",
      "@typescript-eslint/no-misused-promises": "error",
      "n/no-process-exit": "error",
      "@typescript-eslint/no-unused-vars": [
        "error",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
      ],
    },
  },

  {
    // The provider SDKs surface errors as `any`, and the lazily require()d holders are
    // deliberately untyped at the call site. Narrowing those is a separate change.
    files: ["src/file-store.ts"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/no-require-imports": "off",
      // Configure* and the plugin entry are async because Fastify/avvio requires a
      // promise-returning plugin signature, and the FileStore methods must match the
      // interface's Promise return type even when the body has nothing to await.
      "@typescript-eslint/require-await": "off",
    },
  },

  {
    // Mocks are `any` by nature; the type-safety rules fight the test doubles rather
    // than catching anything real.
    files: ["src/**/*.spec.ts"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
      "@typescript-eslint/no-unsafe-call": "off",
      "@typescript-eslint/no-unsafe-return": "off",
      "@typescript-eslint/no-unsafe-argument": "off",
      "@typescript-eslint/unbound-method": "off",
      "@typescript-eslint/no-unsafe-function-type": "off",
      // Specs use require() to re-import modules under jest.isolateModules.
      "@typescript-eslint/no-require-imports": "off",
      "@typescript-eslint/require-await": "off",
    },
  },
);
