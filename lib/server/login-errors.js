// The login failure messages, verbatim from the original POST /login handler
// (lib/server/app.js lines 36789-36858). Keyed so the failure travels as a
// short token in ?error= rather than as attacker-controllable text.

const LOGIN_ERRORS = {
  MISSING: "missing",
  UNAVAILABLE: "unavailable",
  INVALID: "invalid",
  GENERIC: "generic",
};

const LOGIN_ERROR_MESSAGES = {
  [LOGIN_ERRORS.MISSING]: "Please enter phone number and password.",
  [LOGIN_ERRORS.UNAVAILABLE]: "Unable to log in right now. Please try again.",
  [LOGIN_ERRORS.INVALID]: "Invalid phone number or password.",
  [LOGIN_ERRORS.GENERIC]: "Something went wrong while logging in.",
};

function loginErrorMessage(key) {
  return LOGIN_ERROR_MESSAGES[key] || "";
}

export { LOGIN_ERRORS, LOGIN_ERROR_MESSAGES, loginErrorMessage };
