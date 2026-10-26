// Builds everything GitHub Pages serves from docs/, from the one source of truth
// asyncapi/streams-api.asyncapi.yaml:
//   asyncapi/streams-api.asyncapi.json   JSON copy of the spec (committed, for tools that want JSON)
//   docs/index.html                      the page: Scalar API Reference (loaded from jsDelivr) around the
//                                        spec embedded as JSON, same page as the Brand Socket API docs
//   docs/asyncapi.yaml, docs/asyncapi.json
//
//   node scripts/build-docs.mjs           write the files
//   node scripts/build-docs.mjs --check   fail if the committed files are not what the spec produces
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
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

// Pinned: bump on purpose and look at the page. Same version as the Brand Socket API docs.
const SCALAR = "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.73.1";

function page(spec, json) {
  const { title } = spec.info;
  // The spec sits in a <script type="application/json"> element; "<" as \u003c cannot end it early.
  const embedded = json.trimEnd().replace(/</g, "\\u003c");
  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title}</title>
    <meta name="description" content="Socket.IO contract of the ${title}: connection, stream and subscriptions events, and the account token endpoint." />
  </head>
  <body>
    <div id="app"></div>
    <script id="spec" type="application/json">${embedded}</script>
    <script src="${SCALAR}"></script>
    <script>
      Scalar.createApiReference('#app', {
        content: JSON.parse(document.getElementById('spec').textContent),
        hideClientButton: true,
        metaData: { title: ${JSON.stringify(title)} },
      })
    </script>
  </body>
</html>
`;
}

function build(outDir) {
  const yamlText = readFileSync(specYaml, "utf8");
  const spec = parse(yamlText);
  const json = JSON.stringify(spec, null, 2) + "\n";
  return {
    [specJson]: json,
    [join(outDir, "asyncapi.json")]: json,
    [join(outDir, "asyncapi.yaml")]: yamlText,
    [join(outDir, "index.html")]: page(spec, json),
    [join(outDir, ".nojekyll")]: "",
  };
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
