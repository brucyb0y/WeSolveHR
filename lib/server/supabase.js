// Shared Supabase / OpenAI / ffmpeg clients.
//
// Extracted verbatim from the original Express monolith (lib/server/app.js
// lines 199-212). Module-level so every route handler and page reuses one
// client, exactly as the single monolith module instance did.

import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";
import ffmpeg from "fluent-ffmpeg";
import ffmpegStatic from "ffmpeg-static";
import dotenv from "dotenv";

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Missing SUPABASE_URL or usable Supabase key in .env");
}

export const openai = process.env.OPENAI_API_KEY
  ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
  : null;

export const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

ffmpeg.setFfmpegPath(ffmpegStatic);

export { ffmpeg };
