module.exports = {
  roots: ["<rootDir>/src"],
  preset: "ts-jest",
  testEnvironment: "node",
  modulePaths: ["<rootDir>/src"],
  testPathIgnorePatterns: ["<rootDir>/node_modules"],
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        isolatedModules: false,
        // 151002 is ts-jest warning that hybrid module kinds want isolatedModules.
        // Turning that on zeroes function coverage for index.ts, so the warning is
        // silenced instead — type checking itself stays on.
        diagnostics: { ignoreCodes: [151002] },
        // tsconfig.json sets sourceMap:false, which leaves istanbul reporting emitted-JS
        // line numbers — TS parameter properties expand on compile, so the coverage
        // report pointed at imports and comments. Overridden here only; the published
        // build still comes from tsconfig.build.json without source maps.
        tsconfig: { sourceMap: true, types: ["jest", "node"] },
      },
    ],
  },
  collectCoverage: true,
  coverageDirectory: "<rootDir>/coverage",
  coverageReporters: ["text", "lcov"],
  coveragePathIgnorePatterns: ["/node_modules/"],
  // The suite covers every branch today. Without a floor, coverage is reported and
  // then ignored, so a regression lands green.
  coverageThreshold: {
    global: { statements: 100, branches: 100, functions: 100, lines: 100 },
  },
};
