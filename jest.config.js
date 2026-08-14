module.exports = {
  roots: ["<rootDir>/src"],
  preset: "ts-jest",
  testEnvironment: "node",
  bail: 1,
  modulePaths: ["<rootDir>/src"],
  testPathIgnorePatterns: ["<rootDir>/node_modules"],
  transform: {
    "^.+\\.tsx?$": [
      "ts-jest",
      {
        diagnostics: false,
        isolatedModules: false,
        include: [],
        // tsconfig.json sets sourceMap:false, which leaves istanbul reporting emitted-JS
        // line numbers — TS parameter properties expand on compile, so the coverage
        // report pointed at imports and comments. Overridden here only; the published
        // build still comes from tsconfig.build.json without source maps.
        tsconfig: { sourceMap: true },
      },
    ],
  },
  collectCoverage: true,
  coverageDirectory: "<rootDir>/coverage",
  coverageReporters: ["text", "lcov"],
  coveragePathIgnorePatterns: ["/node_modules/"],
};
