// Markup for GET /login.
//
// Body markup extracted verbatim from renderLoginPage() (lib/server/app.js
// lines 1230-1533). The document shell now comes from
// app/layout.jsx, the <style> block from ./login.css, and the inline
// <script> from public/js/.

import { escapeHtml } from "@/lib/ui/html.js";

function renderLoginPage(errorMessage = "") {
  return `
            <div class="login-shell">
          <div class="minimal-top">
            <div class="minimal-top-inner">
              <div class="brand">WeSolveHR</div>
              <div class="brand-sub">Personal workspace login</div>
            </div>
          </div>

          <div class="login-wrap">
            <div class="hero-card">
              <div class="eyebrow">Team Operations</div>
              <h1>Welcome back</h1>
              <p>
                Log in to access your personal workspace, attendance details,
                leave balance, recent feedback, and appraisal history.
              </p>
            </div>

            <div class="login-card">
              <h2>Sign in</h2>
              <div class="login-subtitle">Use your phone number and password to continue.</div>

              ${errorMessage ? `<div class="login-error">${escapeHtml(errorMessage)}</div>` : ""}

<form method="POST" action="/login" id="loginForm">
  <div class="form-group">
<label>Phone number</label>
<input
  class="input"
  type="text"
  name="phone"
  placeholder="e.g. +12133081594 or +919891517965"
  autocomplete="tel"
/>
<p class="helper" style="margin-top:8px;">
  Enter your full phone number with country code.
</p>
  </div>

  <div class="form-group">
    <label class="label">Password</label>
    <input
      class="input"
      type="password"
      name="password"
      placeholder="Enter password"
      autocomplete="current-password"
    />
  </div>

  <button class="login-btn" id="loginSubmitBtn" type="submit">Login</button>
</form>

<div id="loginLoadingOverlay" class="loading-overlay">
  <div class="loading-card">
    <div class="loading-spinner"></div>
    <div style="font-weight:700;">Logging you in...</div>
  </div>
</div>

<script src="/js/login.js"></script>

              <div class="helper">
                First-time users can use the default password assigned by admin. Use your full phone number with country code.
              </div>
            </div>
          </div>
        </div>
      
  `;
}

export {
  renderLoginPage,
};
