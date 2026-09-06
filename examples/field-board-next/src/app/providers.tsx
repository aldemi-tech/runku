"use client";

import { RunkuClient } from "@runku/client";
import { RunkuProvider } from "@runku/react";
import { useMemo, type ReactNode } from "react";
import { baseUrl, publishableKey, target } from "@/lib/config";

export function Providers({ children }: Readonly<{ children: ReactNode }>) {
  const client = useMemo(() => new RunkuClient({
    baseUrl,
    target,
    applicationKey: publishableKey(),
  }), []);
  return <RunkuProvider client={client} identityKey="public-board">{children}</RunkuProvider>;
}
