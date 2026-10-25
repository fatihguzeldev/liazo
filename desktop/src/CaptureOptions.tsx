import { key } from "./api";
import type { Settings, Source } from "./api";

export function CaptureOptions({
  draft,
  sources,
  edit,
}: {
  draft: Settings;
  sources: Source[];
  edit: (settings: Settings) => void;
}) {
  return (
    <>
      {!!draft.images.length && (
        <details>
          <summary>capture</summary>
          {draft.images.map((image, index) => {
            const update = (patch: Partial<typeof image>) =>
              edit({
                ...draft,
                images: draft.images.map((old, position) =>
                  position === index ? { ...old, ...patch } : old,
                ),
              });
            return (
              <fieldset key={key(image.source)}>
                <legend>
                  {sources.find(
                    (source) => key(source.key) === key(image.source),
                  )?.label ?? image.source.id}
                </legend>
                <div className="field-pair">
                  <label>
                    interval · ms
                    <input
                      type="number"
                      min={50}
                      max={60000}
                      value={image.interval_ms}
                      onChange={(event) =>
                        update({ interval_ms: Number(event.target.value) })
                      }
                      required
                    />
                  </label>
                  <label>
                    width · px
                    <input
                      type="number"
                      min={1}
                      max={16384}
                      value={image.max_width ?? ""}
                      placeholder="original"
                      onChange={(event) =>
                        update({
                          max_width: event.target.value
                            ? Number(event.target.value)
                            : null,
                        })
                      }
                    />
                  </label>
                </div>
                <div className="field-pair">
                  <label>
                    format
                    <select
                      value={image.format}
                      onChange={(event) =>
                        update({
                          format: event.target.value as typeof image.format,
                        })
                      }
                    >
                      <option value="png">png</option>
                      <option value="jpeg">jpeg</option>
                    </select>
                  </label>
                  <label>
                    quality
                    <input
                      type="number"
                      min={1}
                      max={100}
                      value={image.quality}
                      onChange={(event) =>
                        update({ quality: Number(event.target.value) })
                      }
                      required
                    />
                  </label>
                </div>
                <label className="inline">
                  <input
                    type="checkbox"
                    checked={!!image.crop}
                    onChange={(event) =>
                      update({
                        crop: event.target.checked
                          ? { x: 0, y: 0, width: 800, height: 600 }
                          : null,
                      })
                    }
                  />
                  crop
                </label>
                {image.crop && (
                  <div className="crop-fields">
                    {(["x", "y", "width", "height"] as const).map((field) => (
                      <label key={field}>
                        {field}
                        <input
                          type="number"
                          min={field === "x" || field === "y" ? 0 : 1}
                          value={image.crop![field]}
                          onChange={(event) =>
                            update({
                              crop: {
                                ...image.crop!,
                                [field]: Number(event.target.value),
                              },
                            })
                          }
                          required
                        />
                      </label>
                    ))}
                  </div>
                )}
                <label className="inline">
                  <input
                    type="checkbox"
                    checked={image.include_cursor}
                    onChange={(event) =>
                      update({ include_cursor: event.target.checked })
                    }
                  />
                  cursor
                </label>
              </fieldset>
            );
          })}
        </details>
      )}
    </>
  );
}
