"use client";

// Help & Commands guide. Ported from renderHelpPage() in lib/server/app.js: the
// command sections render from HELP_SECTIONS, each command is click-to-copy, and
// the search box live-filters rows/sections — all React state now instead of the
// inline copy + filter scripts.

import { useMemo, useState } from "react";
import { HELP_SECTIONS } from "./sections.js";
import styles from "./help.module.css";

function cmdText(c) {
  return typeof c === "string" ? c : c.cmd;
}
function cmdDesc(c) {
  return typeof c === "string" ? "" : c.desc;
}

export default function HelpGuide({ isAdmin }) {
  const [search, setSearch] = useState("");
  const [copiedKey, setCopiedKey] = useState(null);

  const visibleSections = useMemo(
    () => HELP_SECTIONS.filter((s) => !s.admin || isAdmin),
    [isAdmin],
  );

  const term = search.trim().toLowerCase();

  function rowMatches(section, c) {
    if (!term) return true;
    if (section.title.toLowerCase().includes(term)) return true;
    const text = (cmdText(c) + " " + (cmdDesc(c) || "")).toLowerCase();
    return text.includes(term);
  }

  function sectionVisible(section) {
    if (!term) return true;
    if (section.title.toLowerCase().includes(term)) return true;
    return section.subs.some((sub) => sub.commands.some((c) => rowMatches(section, c)));
  }

  const anySection = visibleSections.some(sectionVisible);

  async function copyCmd(key, text) {
    const done = () => {
      setCopiedKey(key);
      setTimeout(() => setCopiedKey((k) => (k === key ? null : k)), 900);
    };
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      }
    } finally {
      done();
    }
  }

  return (
    <div className={styles.wrap}>
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
              {s.admin ? <span className={styles.helpTocBadge}>Admin</span> : null}
            </a>
          ))}
        </nav>

        <div>
          <div className={styles.helpSections}>
            {visibleSections.map((sec) => (
              <section
                className={styles.helpSection}
                id={sec.id}
                key={sec.id}
                style={sectionVisible(sec) ? undefined : { display: "none" }}
              >
                <div className={styles.helpSectionHead}>
                  <div className={styles.helpSectionIcon} aria-hidden="true">
                    {sec.icon}
                  </div>
                  <div className={styles.helpSectionTitles}>
                    <h2>{sec.title}</h2>
                    {sec.blurb ? (
                      <div className={styles.helpSectionBlurb}>{sec.blurb}</div>
                    ) : null}
                  </div>
                  {sec.admin ? (
                    <span className={styles.helpAdminBadge}>Manager / Admin</span>
                  ) : null}
                </div>

                <div className={styles.helpSectionBody}>
                  {sec.subs.map((sub, si) => (
                    <div key={si}>
                      {sub.label ? (
                        <div className={styles.helpSubLabel}>{sub.label}</div>
                      ) : null}
                      <div className={styles.helpCmdList}>
                        {sub.commands.map((c, ci) => {
                          const key = `${sec.id}-${si}-${ci}`;
                          const text = cmdText(c);
                          const desc = cmdDesc(c);
                          return (
                            <div
                              className={styles.helpCmdRow}
                              key={ci}
                              style={
                                rowMatches(sec, c) ? undefined : { display: "none" }
                              }
                            >
                              <button
                                className={`${styles.helpCmd} ${copiedKey === key ? styles.copied : ""}`}
                                type="button"
                                title="Click to copy"
                                onClick={() => copyCmd(key, text)}
                              >
                                {copiedKey === key ? "Copied!" : text}
                              </button>
                              {desc ? (
                                <span className={styles.helpCmdDesc}>{desc}</span>
                              ) : null}
                            </div>
                          );
                        })}
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
          {!anySection ? (
            <div className={styles.helpEmpty}>No commands match your search.</div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
