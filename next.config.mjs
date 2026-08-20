/** @type {import('next').NextConfig} */
const nextConfig = {
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
