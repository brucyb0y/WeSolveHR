// Client script for GET /bugs, extracted verbatim from the
// inline <script> of renderStage0BugBoardPage() (lib/server/app.js lines 1535-1841).
async function createBug() {
  const title = document.getElementById("bugTitle").value.trim();
  const description = document.getElementById("bugDescription").value.trim();
  const board_column = document.getElementById("bugColumn").value;
  const severity = document.getElementById("bugSeverity").value;
  const source_message_sid = document.getElementById("bugSourceSid").value.trim();
  const source_phone_number = document.getElementById("bugSourcePhone").value.trim();
  const source_message_text = document.getElementById("bugSourceText").value.trim();

  if (!title) {
    alert("Title is required");
    return;
  }

  const res = await fetch("/api/bugs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      title,
      description,
      board_column,
      severity,
      source_message_sid,
      source_phone_number,
      source_message_text
    })
  });

  const json = await res.json();
  if (!json.ok) {
    alert(json.error || "Failed to create bug");
    return;
  }

  location.reload();
}

async function updateBug(id, patch) {
  const res = await fetch("/api/bugs/" + id, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch)
  });

  const json = await res.json();
  if (!json.ok) {
    alert(json.error || "Failed to update bug");
    return;
  }

  location.reload();
}
