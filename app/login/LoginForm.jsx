"use client";

// Client component for the login form. Drives the loginAction via useActionState,
// which gives us the inline error message and the pending state used for the
// button label + the loading overlay (replacing the hand-written DOM script in
// the original renderLoginPage).

import { useActionState } from "react";
import { loginAction } from "./actions.js";
import styles from "./login.module.css";

const initialState = { error: "" };

export default function LoginForm() {
  const [state, formAction, isPending] = useActionState(
    loginAction,
    initialState,
  );

  return (
    <div className={styles.loginShell}>
      <div className={styles.minimalTop}>
        <div className={styles.minimalTopInner}>
          <div className={styles.brand}>WeSolveHR</div>
          <div className={styles.brandSub}>Personal workspace login</div>
        </div>
      </div>

      <div className={styles.loginWrap}>
        <div className={styles.heroCard}>
          <div className={styles.eyebrow}>Team Operations</div>
          <h1 className={styles.heroTitle}>Welcome back</h1>
          <p className={styles.heroText}>
            Log in to access your personal workspace, attendance details, leave
            balance, recent feedback, and appraisal history.
          </p>
        </div>

        <div className={styles.loginCard}>
          <h2 className={styles.cardTitle}>Sign in</h2>
          <div className={styles.subtitle}>
            Use your phone number and password to continue.
          </div>

          {state?.error ? (
            <div className={styles.loginError}>{state.error}</div>
          ) : null}

          <form action={formAction}>
            <div className={styles.formGroup}>
              <label className={styles.label}>Phone number</label>
              <input
                className={styles.input}
                type="text"
                name="phone"
                placeholder="e.g. +12133081594 or +919891517965"
                autoComplete="tel"
              />
              <p className={styles.helper} style={{ marginTop: 8 }}>
                Enter your full phone number with country code.
              </p>
            </div>

            <div className={styles.formGroup}>
              <label className={styles.label}>Password</label>
              <input
                className={styles.input}
                type="password"
                name="password"
                placeholder="Enter password"
                autoComplete="current-password"
              />
            </div>

            <button className={styles.loginBtn} type="submit" disabled={isPending}>
              {isPending ? "Logging in..." : "Login"}
            </button>
          </form>

          <div className={styles.helper}>
            First-time users can use the default password assigned by admin. Use
            your full phone number with country code.
          </div>
        </div>
      </div>

      {isPending ? (
        <div className={styles.loadingOverlay}>
          <div className={styles.loadingCard}>
            <div className={styles.loadingSpinner}></div>
            <div style={{ fontWeight: 700 }}>Logging you in...</div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
