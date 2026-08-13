// /login — replaces renderLoginPage() + app.get("/login").
// The POST half now lives in ./actions.js as a Server Action.

import LoginForm from "./LoginForm";
import styles from "./login.module.css";

export const metadata = { title: "Login | WeSolveHR" };

export default function LoginPage() {
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
          <h1>Welcome back</h1>
          <p>
            Log in to access your personal workspace, attendance details, leave
            balance, recent feedback, and appraisal history.
          </p>
        </div>

        <div className={styles.loginCard}>
          <h2>Sign in</h2>
          <div className={styles.loginSubtitle}>
            Use your phone number and password to continue.
          </div>

          <LoginForm />

          <div className={styles.helper}>
            First-time users can use the default password assigned by admin. Use
            your full phone number with country code.
          </div>
        </div>
      </div>
    </div>
  );
}
