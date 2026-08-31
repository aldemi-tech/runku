import type { Metadata } from "next"
import type { ReactNode } from "react"

import "./styles.css"

export const metadata: Metadata = {
  title: "Runku Chat",
  description: "End-to-end authentication, data, and realtime example for Runku",
}

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      {/* Browser extensions may add attributes to body before React hydrates it. */}
      <body suppressHydrationWarning>{children}</body>
    </html>
  )
}
