import "server-only";

import { RunkuClient } from "@runku/client";
import { createRunkuReactServer } from "@runku/react/server";
import { baseUrl, publishableKey, target } from "./config";

export function createServerRunku() {
  const applicationKey = process.env.RUNKU_SECRET_KEY ?? publishableKey();
  return createRunkuReactServer(new RunkuClient({ baseUrl, target, applicationKey }), "public-board");
}
