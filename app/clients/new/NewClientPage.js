// Markup for GET /clients/new.
//
// Body markup extracted verbatim from renderNewClientPage() (lib/server/app.js
// lines 3736-3958). The document shell now comes from
// app/layout.jsx, the <style> block from ./new-client.css, and the inline
// <script> from public/js/.

import { renderTopNav } from "@/lib/ui/nav.js";

function renderNewClientPage() {
  return `
            ${renderTopNav("clients")}

        <div class="wrap">
          <div class="topbar">
            <div>
              <div class="eyebrow">Create Client</div>
              <h1>New Client</h1>
              <div class="subtitle">This page is the starting shell. Database save will come after DB tables are added.</div>
            </div>
            <a class="btn" href="/clients">← Back to Clients</a>
          </div>

          <form method="POST" action="/api/clients">
            <div class="panel">
              <h2>Basic Info</h2>
              <div class="grid">
                <div class="field">
                  <label>Client Name</label>
                  <input name="name" placeholder="Example: Everloop" />
                </div>

                <div class="field">
                  <label>Company Name</label>
                  <input name="company_name" placeholder="Example: Everloop AI Inc." />
                </div>

              </div>

              <div class="field" style="margin-top:14px;">
                <label>Description</label>
                <textarea name="description" placeholder="Short internal description of this client relationship..."></textarea>
              </div>
            </div>

            <div class="panel">
              <h2>Services</h2>
              <div class="grid">
                <label><input type="checkbox" name="services" value="Tech" /> Tech</label>
                <label><input type="checkbox" name="services" value="Sales" /> Sales</label>
                <label><input type="checkbox" name="services" value="Marketing" /> Marketing</label>
                <label><input type="checkbox" name="services" value="GTM" /> GTM</label>
                <label><input type="checkbox" name="services" value="Design" /> Design</label>
                <label><input type="checkbox" name="services" value="QA" /> QA</label>
                <label><input type="checkbox" name="services" value="Operations" /> Operations</label>
                <label><input type="checkbox" name="services" value="Support" /> Support</label>
              </div>
            </div>

            <div class="panel">
              <h2>Primary Client Contact</h2>
              <div class="grid">
                <div class="field">
                  <label>Name</label>
                  <input name="contact_name" placeholder="Client contact name" />
                </div>
        

                <div class="field">
                  <label>Email</label>
                  <input name="contact_email" placeholder="email@example.com" />
                </div>

                <div class="field">
                  <label>Phone</label>
                  <input name="contact_phone" placeholder="+1..." />
                </div>

                <div class="field">
                  <label>Role</label>
                  <input name="contact_role" placeholder="Founder / CEO / PM" />
                </div>
              </div>
            </div>
            
            <div class="panel">
  <div class="actions">
    <a class="btn" href="/clients">Cancel</a>
    <button class="btn btn-primary" type="submit">Create Client</button>
  </div>
</div>
</form>
        </div>
      
  `;
}

export {
  renderNewClientPage,
};
