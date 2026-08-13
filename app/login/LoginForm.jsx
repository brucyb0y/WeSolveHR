"use client";

// The sign-in form. The old page shipped an IIFE that disabled the button and
// toggled a .show class on a loading overlay during submit; that is now driven
// by the action's own pending state.

import { useActionState } from "react";
import { loginAction } from "./actions";
import styles from "./login.module.css";

const INITIAL_STATE = { error: "" };

export default function LoginForm() {
  const [state, formAction, isPending] = useActionState(
    loginAction,
    INITIAL_STATE,
  );

  return (
    <>
      {state?.error ? (
        <div className={styles.loginError}>{state.error}</div>
      ) : null}

      <form action={formAction}>
        <div className={styles.formGroup}>
          {/* Deliberately bare: the original markup omitted class="label" on
              this one field, so it renders unstyled. Preserved to keep the
              migration visually identical. */}
          <label>Phone number</label>
          <input
            className={styles.input}
            type="text"
            name="phone"
            placeholder="e.g. +12133081594 or +919891517965"
            autoComplete="tel"
          />
          <p className={`${styles.helper} ${styles.phoneHelper}`}>
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

      <div
        className={`${styles.loadingOverlay} ${isPending ? styles.show : ""}`}
      >
        <div className={styles.loadingCard}>
          <div className={styles.loadingSpinner} />
          <div className={styles.loadingLabel}>Logging you in...</div>
        </div>
      </div>
    </>
  );
}
