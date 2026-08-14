import FpFileStore from "./file-store";
// Side-effect import. Module augmentations only reach consumers if the declaring file
// is in their compilation; nothing referenced ./types before, so even the existing
// FastifySchema augmentation was never applied.
import "./types";

export { FileStore } from "./file-store";

export const Plugins = {
  FileStore: FpFileStore,
};
