// User role helpers, extracted verbatim from the original monolith.

function getPostLoginRedirectPath(user) {
  if (isManagerOrAdmin(user)) {
    return "/dashboard";
  }

  return "/my-dashboard";
}

function isManagerOrAdmin(user) {
  return user?.role === "admin" || user?.role === "manager";
}

export {
  getPostLoginRedirectPath,
  isManagerOrAdmin,
};
