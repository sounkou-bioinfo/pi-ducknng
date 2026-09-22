import { createHash } from "node:crypto";
import { access, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

/**
 * Finds the ducknng DuckDB extension this package loads, in order:
 *
 * 1. DUCKNNG_EXTENSION_PATH, used as given;
 * 2. a build made by `make ducknng-extension` in a source checkout;
 * 3. the binary shipped in the npm package under prebuilt/<platform>/;
 * 4. the pinned release asset, downloaded once into the user cache and
 *    accepted only if its SHA-256 matches DEPENDENCIES.
 */
export type DucknngPin = {
  ref: string;
  releaseUrl: string;
  sha256: Record<string, string>;
};

const EXTENSION_FILE = "ducknng.duckdb_extension";
const SUPPORTED = ["linux_amd64", "linux_arm64", "osx_amd64", "osx_arm64"];
const MIN_GLIBC = [2, 28];
const resolving = new Map<string, Promise<string>>();

export async function readDucknngPin(root: string): Promise<DucknngPin> {
  const text = await readFile(resolve(root, "DEPENDENCIES"), "utf8");
  const values = new Map<string, string>();
  for (const line of text.split("\n")) {
    const match = /^([A-Z0-9_]+)=(.*)$/.exec(line.trim());
    if (match) values.set(match[1], match[2]);
  }
  const ref = values.get("DUCKNNG_REF");
  const releaseUrl = values.get("DUCKNNG_RELEASE_URL");
  if (!ref || !releaseUrl) throw new Error("DEPENDENCIES does not pin DUCKNNG_REF and DUCKNNG_RELEASE_URL");
  const sha256: Record<string, string> = {};
  for (const platform of SUPPORTED) {
    const digest = values.get(`DUCKNNG_SHA256_${platform.toUpperCase()}`);
    if (digest) sha256[platform] = digest;
  }
  return { ref, releaseUrl, sha256 };
}

/** The DuckDB platform name of this process, or an error naming why not. */
export function ducknngPlatform(): string {
  const arch = { x64: "amd64", arm64: "arm64" }[process.arch as string];
  if (process.platform === "darwin" && arch) return `osx_${arch}`;
  if (process.platform === "linux" && arch) {
    const header = (process.report?.getReport() as { header?: { glibcVersionRuntime?: string } })?.header;
    const glibc = header?.glibcVersionRuntime;
    if (!glibc) {
      throw new Error("pi-ducknng needs a glibc Linux; musl builds such as Alpine are not supported");
    }
    const [major, minor] = glibc.split(".").map(Number);
    if (major < MIN_GLIBC[0] || (major === MIN_GLIBC[0] && minor < MIN_GLIBC[1])) {
      throw new Error(`pi-ducknng needs glibc ${MIN_GLIBC.join(".")} or newer; this system has ${glibc}`);
    }
    return `linux_${arch}`;
  }
  throw new Error(
    `pi-ducknng has no ducknng build for ${process.platform} ${process.arch}; ` +
    `supported: ${SUPPORTED.join(", ")}`,
  );
}

function cacheRoot(): string {
  if (process.platform === "darwin") return join(homedir(), "Library", "Caches", "pi-ducknng");
  const xdg = process.env.XDG_CACHE_HOME?.trim();
  return join(xdg || join(homedir(), ".cache"), "pi-ducknng");
}

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function sha256(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** Downloads the pinned asset for platform into the cache, verified. */
async function downloadPinned(pin: DucknngPin, platform: string): Promise<string> {
  const expected = pin.sha256[platform];
  if (!expected) throw new Error(`DEPENDENCIES has no checksum for ${platform}`);
  const directory = join(cacheRoot(), pin.ref, platform);
  const target = join(directory, EXTENSION_FILE);
  if (await exists(target) && sha256(await readFile(target)) === expected) return target;
  const url = `${pin.releaseUrl}/${pin.ref}/ducknng-${pin.ref}-${platform}.duckdb_extension`;
  let response: Response;
  try {
    response = await fetch(url, { signal: AbortSignal.timeout(120_000) });
  } catch (error) {
    throw new Error(
      `could not download ducknng ${pin.ref} for ${platform} from ${url}: ` +
      `${error instanceof Error ? error.message : String(error)}. ` +
      "Set DUCKNNG_EXTENSION_PATH to a ducknng build for DuckDB 1.5.4, or run " +
      "`make ducknng-extension` in a source checkout.",
    );
  }
  if (!response.ok) throw new Error(`could not download ${url}: HTTP ${response.status}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  const actual = sha256(bytes);
  if (actual !== expected) {
    throw new Error(`refusing ${url}: SHA-256 ${actual} does not match the pinned ${expected}`);
  }
  await mkdir(directory, { recursive: true });
  const staged = `${target}.${process.pid}.tmp`;
  await writeFile(staged, bytes, { mode: 0o644 });
  await rename(staged, target);
  return target;
}

async function resolveUncached(root: string): Promise<string> {
  const configured = process.env.DUCKNNG_EXTENSION_PATH?.trim();
  if (configured) {
    const path = resolve(configured);
    if (!(await exists(path))) throw new Error(`DUCKNNG_EXTENSION_PATH does not exist: ${path}`);
    return path;
  }
  const built = resolve(root, "vendor/ducknng/build/release", EXTENSION_FILE);
  if (await exists(built)) return built;
  const platform = ducknngPlatform();
  const pin = await readDucknngPin(root);
  const bundled = resolve(root, "prebuilt", platform, EXTENSION_FILE);
  if (await exists(bundled)) {
    const expected = pin.sha256[platform];
    if (expected && sha256(await readFile(bundled)) !== expected) {
      throw new Error(`the bundled ${bundled} does not match its pinned SHA-256; reinstall pi-ducknng`);
    }
    return bundled;
  }
  return await downloadPinned(pin, platform);
}

/** Resolves once per root per process; a failure is retried on the next call. */
export async function resolveDucknngExtension(root: string): Promise<string> {
  let pending = resolving.get(root);
  if (!pending) {
    pending = resolveUncached(root);
    resolving.set(root, pending);
    pending.catch(() => {
      if (resolving.get(root) === pending) resolving.delete(root);
    });
  }
  return await pending;
}
