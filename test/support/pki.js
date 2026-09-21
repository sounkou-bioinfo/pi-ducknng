import { execFileSync } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

function openssl(args, cwd) {
  execFileSync("openssl", args, { cwd, stdio: "ignore" });
}

async function issue(dir, name, subject, extension) {
  openssl(["req", "-newkey", "rsa:2048", "-nodes", "-keyout", `${name}.key`,
    "-out", `${name}.csr`, "-subj", subject], dir);
  await writeFile(resolve(dir, `${name}.ext`), extension);
  openssl(["x509", "-req", "-in", `${name}.csr`, "-CA", "ca.pem", "-CAkey", "ca.key",
    "-CAcreateserial", "-out", `${name}.crt`, "-days", "1", "-extfile", `${name}.ext`], dir);
  const combined = resolve(dir, `${name}.pem`);
  await writeFile(
    combined,
    (await readFile(resolve(dir, `${name}.crt`), "utf8")) +
      (await readFile(resolve(dir, `${name}.key`), "utf8")),
  );
  return combined;
}

/** Issues a throwaway CA, a 127.0.0.1 server certificate, and client certificates. */
export async function pki(dir, clients = ["pi-agent"]) {
  openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key",
    "-out", "ca.pem", "-days", "1", "-subj", "/CN=pi-ducknng-test-ca"], dir);
  const issued = {
    ca: resolve(dir, "ca.pem"),
    server: await issue(dir, "server", "/CN=127.0.0.1",
      "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"),
  };
  for (const name of clients) {
    issued[name] = await issue(dir, `client-${name}`, `/CN=${name}`,
      "extendedKeyUsage=clientAuth\n");
  }
  return issued;
}

/** Runs operation with environment overrides, restoring them afterwards. */
export async function withEnv(values, operation) {
  const previous = {};
  for (const [name, value] of Object.entries(values)) {
    previous[name] = process.env[name];
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  try {
    return await operation();
  } finally {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}
