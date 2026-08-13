"use client";

// Excel lead import — replaces openLeadImportModal / closeLeadImportModal /
// uploadClientLeadsExcel.
//
// The chosen Category Type is stamped on EVERY row of the sheet, which is why
// it is a single dropdown and not a per-row column: the Leads tab's category
// chips are built from it, so a sheet imported without one produces leads that
// no chip can reach.
//
// It is required for most clients but optional for Revivflow, whose sheet
// carries its own free-text ICP Category that lands in icp_category instead.
// `categoryTypeRequired` comes from the server rather than being decided here.
//
// A rejected sheet comes back with a multi-line, row-by-row explanation (an
// unknown "Assigned to" name, say). That text is shown as-is and the file is
// cleared so it can be fixed and re-uploaded — the failure is actionable, not
// just a status code.

import { useRef, useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import { WorkModal, Field, SelectField } from "./WorkModal";

export default function LeadImportModal({
  clientId,
  categoryTypes,
  categoryTypeRequired,
  onClose,
}) {
  const router = useRouter();
  const fileRef = useRef(null);
  const [categoryType, setCategoryType] = useState("");
  const [saving, setSaving] = useState(false);

  const options = [
    { value: "", label: categoryTypeRequired ? "Select…" : "None" },
    ...categoryTypes.map((c) => ({ value: c.key, label: c.label })),
  ];

  async function upload() {
    const file = fileRef.current?.files?.[0];
    if (!file) {
      alert("Choose an Excel file first.");
      return;
    }
    if (categoryTypeRequired && !categoryType) {
      alert("Select a category type first.");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);
    // Every row in the sheet is stamped with this category type.
    formData.append("category_type", categoryType);

    setSaving(true);
    Swal.fire({
      title: "Importing leads from Excel...",
      allowOutsideClick: false,
      didOpen: () => Swal.showLoading(),
    });

    try {
      const res = await fetch(`/api/clients/${clientId}/leads/import-excel`, {
        method: "POST",
        body: formData,
      });
      const json = await res.json();
      Swal.close();

      if (!json.ok) {
        // Preserved verbatim: the server's row-by-row rejection text is the
        // whole value of this message.
        alert(`Import failed: \n${json.error || "Unknown error"}`);
        if (fileRef.current) fileRef.current.value = "";
        return;
      }

      const d = json.data || {};
      alert(
        [
          "Import complete",
          `Total rows: ${d.total}`,
          `Inserted: ${d.inserted}`,
          `Updated (existing email): ${d.updated || 0}`,
          `Duplicates skipped: ${d.duplicates}`,
          `Empty skipped: ${d.skipped}`,
          `Errors: ${(d.errors || []).length}`,
        ].join("\n"),
      );

      onClose();
      router.refresh();
    } catch {
      Swal.close();
      alert("Import failed");
    } finally {
      setSaving(false);
    }
  }

  return (
    <WorkModal
      title="Import Leads from Excel"
      saveLabel="Import"
      saving={saving}
      onSave={upload}
      onClose={onClose}
    >
      <Field label="Excel File" wide>
        <input ref={fileRef} type="file" accept=".xlsx,.xls,.csv" />
      </Field>

      <SelectField
        label={`Category Type${categoryTypeRequired ? "" : " (optional)"}`}
        options={options}
        value={categoryType}
        onChange={setCategoryType}
      />
    </WorkModal>
  );
}
