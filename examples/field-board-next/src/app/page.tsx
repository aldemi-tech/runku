import { RunkuHydrationBoundary } from "@runku/react";
import { serverApi } from "../../runku/_generated/server.js";
import { createServerRunku } from "@/lib/runku-server";
import { FieldBoard } from "./field-board";
import { Providers } from "./providers";

export const dynamic = "force-dynamic";

export default async function Page() {
  const runku = createServerRunku();
  await runku.preloadQuery(serverApi.tasks.list, null);
  return (
    <RunkuHydrationBoundary state={runku.dehydrate()}>
      <Providers><FieldBoard /></Providers>
    </RunkuHydrationBoundary>
  );
}
