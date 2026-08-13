"use client";

// Shared pieces for both report views: the per-task sentence with its clickable
// "#N" chip, and the task-updates / extra-work sections of a report card.
//
// linkifyTaskSentence() used to splice a <button onclick="openTaskDetail(n)">
// into an escaped HTML string. Here the sentence is split on the same
// /^Task #\d+/ prefix and the chip is a real element, so nothing is injected.

export function TaskSentence({ styles, sentence, taskNo, onOpen }) {
  const text = sentence || "";
  const match = text.match(/^Task #\d+/);

  if (!match) return text;

  return (
    <>
      Task{" "}
      <button
        type="button"
        className={styles.taskInlineLink}
        onClick={() => onOpen(taskNo)}
      >
        #{taskNo}
      </button>
      {text.slice(match[0].length)}
    </>
  );
}

export function TaskUpdates({ styles, narratives, onOpen, emptyText }) {
  if (!narratives || !narratives.length) {
    return <li className="muted">{emptyText}</li>;
  }

  return narratives.map((item, i) => (
    <li className={styles.reportTaskItem} key={item.taskId ?? i}>
      <div className={styles.taskLine}>
        <TaskSentence
          styles={styles}
          sentence={item.sentence}
          taskNo={item.taskNo}
          onOpen={onOpen}
        />
      </div>
      {(item.compactChanges || []).length ? (
        <div className={styles.changeChips}>
          {item.compactChanges.map((chip, j) => (
            <span
              className={styles.changeChip}
              title={chip.detail || chip.label}
              key={j}
            >
              {chip.label}
            </span>
          ))}
        </div>
      ) : null}
    </li>
  ));
}

export function ExtraWork({ notes }) {
  if (!notes || !notes.length) {
    return <li className="muted">No extra work notes</li>;
  }
  return notes.map((note, i) => <li key={i}>{note}</li>);
}
