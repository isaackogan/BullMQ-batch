import {cpSync, mkdirSync} from "node:fs";

mkdirSync("dist/commands", {recursive: true});
cpSync("src/commands", "dist/commands", {
    recursive: true,
    filter: (p) => !p.endsWith(".ts"),
});
