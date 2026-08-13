"use client";

// Command guide: click-to-copy buttons plus the live search filter.
//
// Filtering matches the original exactly, including its one deliberate
// looseness: when a section's TITLE matches the term, every command in it stays
// visible, and sub-labels/notes are never hidden even if all the rows beneath
// them are ("hiding those is overkill" — the original's own comment). The
// table of contents is likewise not filtered.

import { useMemo, useState } from "react";
import { HELP_SECTIONS } from "@/lib/data/help-sections";
import styles from "./help.module.css";

const COPIED_MS = 900;

const commandText = (c) => (typeof c === "string" ? c : c.cmd);
const commandDesc = (c) => (typeof c === "string" ? "" : c.desc || "");

function CommandButton({ command }) {
  const [copied, setCopied] = useState(false);
  const text = commandText(command);
  const desc = commandDesc(command);

  function copy() {
    const done = () => {
      setCopied(true);
      setTimeout(() => setCopied(false), COPIED_MS);
    };

    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(text).then(done, done);
    } else {
      done();
    }
  }

  return (
    <div className={styles.helpCmdRow}>
      <button
        className={`${styles.helpCmd} ${copied ? styles.copied : ""}`}
        type="button"
        title="Click to copy"
        onClick={copy}
      >
        {copied ? "Copied!" : text}
      </button>
      {desc ? <span className={styles.helpCmdDesc}>{desc}</span> : null}
    </div>
  );
}

export default function HelpContent({ isAdmin }) {
  const [search, setSearch] = useState("");

  const visibleSections = useMemo(
    () => HELP_SECTIONS.filter((s) => !s.admin || isAdmin),
    [isAdmin],
  );

  const term = search.trim().toLowerCase();

  const results = useMemo(() => {
    return visibleSections
      .map((section) => {
        const titleMatch = section.title.toLowerCase().includes(term);

        const subs = section.subs.map((sub) => ({
          ...sub,
          commands: sub.commands.filter((c) => {
            if (!term || titleMatch) return true;
            const haystack =
              `${commandText(c)}${commandDesc(c)}`.toLowerCase();
            return haystack.includes(term);
          }),
        }));

        const anyRow = subs.some((s) => s.commands.length > 0);

        return { section, subs, visible: !term || titleMatch || anyRow };
      })
      .filter((r) => r.visible);
  }, [visibleSections, term]);

  return (
    <>
      <div className={styles.helpHero}>
        <div className={styles.eyebrow}>Help Center</div>
        <h1>WeSolveHR Command Guide</h1>
        <p>
          Everything you can do by chatting with the assistant — attendance,
          tasks, feedback and more. Tap any command to copy it.
        </p>
        <div className={styles.helpSearchBox}>
          <span className={styles.helpSearchIco} aria-hidden="true">
            🔍
          </span>
          <input
            type="search"
            placeholder="Search commands… (e.g. break, leave, create task)"
            autoComplete="off"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className={styles.helpLayout}>
        <nav className={styles.helpToc} aria-label="Sections">
          {visibleSections.map((s) => (
            <a className={styles.helpTocLink} href={`#${s.id}`} key={s.id}>
              <span className={styles.helpTocIco}>{s.icon}</span>
              {s.title}
              {s.admin ? (
                <span className={styles.helpTocBadge}>Admin</span>
              ) : null}
            </a>
          ))}
        </nav>

        <div>
          <div className={styles.helpSections}>
            {results.map(({ section, subs }) => (
              <section
                className={styles.helpSection}
                id={section.id}
                key={section.id}
              >
                <div className={styles.helpSectionHead}>
                  <div className={styles.helpSectionIcon} aria-hidden="true">
                    {section.icon}
                  </div>
                  <div className={styles.helpSectionTitles}>
                    <h2>{section.title}</h2>
                    {section.blurb ? (
                      <div className={styles.helpSectionBlurb}>
                        {section.blurb}
                      </div>
                    ) : null}
                  </div>
                  {section.admin ? (
                    <span className={styles.helpAdminBadge}>
                      Manager / Admin
                    </span>
                  ) : null}
                </div>

                <div className={styles.helpSectionBody}>
                  {subs.map((sub, i) => (
                    <div key={sub.label || i}>
                      {sub.label ? (
                        <div className={styles.helpSubLabel}>{sub.label}</div>
                      ) : null}
                      <div className={styles.helpCmdList}>
                        {sub.commands.map((c) => (
                          <CommandButton command={c} key={commandText(c)} />
                        ))}
                      </div>
                      {sub.note ? (
                        <div className={styles.helpNote}>{sub.note}</div>
                      ) : null}
                    </div>
                  ))}
                </div>
              </section>
            ))}
          </div>

          {results.length === 0 ? (
            <div className={styles.helpEmpty}>
              No commands match your search.
            </div>
          ) : null}
        </div>
      </div>
    </>
  );
}
