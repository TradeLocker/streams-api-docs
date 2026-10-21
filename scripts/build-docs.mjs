// Builds everything GitHub Pages serves from docs/, from the one source of truth
// asyncapi/streams-api.asyncapi.yaml:
//   asyncapi/streams-api.asyncapi.json   JSON copy of the spec (committed, for tools that want JSON)
//   docs/index.html                      rendered AsyncAPI page (single file, works offline)
//   docs/asyncapi.yaml, docs/asyncapi.json
//
//   node scripts/build-docs.mjs           write the files
//   node scripts/build-docs.mjs --check   fail if the committed files are not what the spec produces
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const specYaml = join(root, "asyncapi", "streams-api.asyncapi.yaml");
const specJson = join(root, "asyncapi", "streams-api.asyncapi.json");
const docs = join(root, "docs");
const check = process.argv.includes("--check");

function cli(...args) {
  execFileSync(join(root, "node_modules", ".bin", "asyncapi"), args, {
    cwd: root,
    stdio: ["ignore", "ignore", "inherit"],
    env: { ...process.env, SUPPRESS_NO_CONFIG_WARNING: "1", NODE_NO_WARNINGS: "1" },
  });
}

function build(outDir) {
  const yamlText = readFileSync(specYaml, "utf8");
  const json = JSON.stringify(parse(yamlText), null, 2) + "\n";

  const rendered = mkdtempSync(join(tmpdir(), "streams-api-docs-"));
  try {
    cli(
      "generate",
      "fromTemplate",
      specYaml,
      join(root, "node_modules", "@asyncapi", "html-template"),
      "--output",
      rendered,
      "--force-write",
      "--no-interactive",
      "--param",
      "singleFile=true",
    );
    return {
      [specJson]: json,
      [join(outDir, "asyncapi.json")]: json,
      [join(outDir, "asyncapi.yaml")]: yamlText,
      [join(outDir, "index.html")]: readFileSync(join(rendered, "index.html"), "utf8"),
      [join(outDir, ".nojekyll")]: "",
    };
  } finally {
    rmSync(rendered, { recursive: true, force: true });
  }
}

cli("validate", specYaml, "--fail-severity=error");
const files = build(docs);

if (check) {
  const stale = Object.entries(files)
    .filter(([path, content]) => !existsSync(path) || readFileSync(path, "utf8") !== content)
    .map(([path]) => path.slice(root.length + 1));
  if (stale.length > 0) {
    console.error(`Out of date, run \`npm run build\` and commit:\n  ${stale.join("\n  ")}`);
    process.exit(1);
  }
  console.log("docs/ and asyncapi/*.json match the spec.");
} else {
  for (const [path, content] of Object.entries(files)) {
    writeFileSync(path, content);
  }
  console.log(`Wrote ${Object.keys(files).map((p) => p.slice(root.length + 1)).join(", ")}`);
}
