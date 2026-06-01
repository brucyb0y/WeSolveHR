/** @type {import('next').NextConfig} */
const nextConfig = {
  // These packages use native bindings / ship binaries / are large server-only
  // SDKs. Keep them external so Next does not try to bundle them into the
  // route-handler chunks (matches how they ran under plain Node).
  serverExternalPackages: [
    "bcrypt",
    "ffmpeg-static",
    "fluent-ffmpeg",
    "@supabase/supabase-js",
    "openai",
    "twilio",
    "xlsx",
  ],
};

export default nextConfig;
