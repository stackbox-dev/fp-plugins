import FpEventBus from "./event-bus";
import FpFileStore from "./file-store";

export * from "./event-bus/interfaces";
export { FileStore } from "./file-store";

export const Plugins = {
  EventBus: FpEventBus,
  FileStore: FpFileStore,
};

export * from "./event-bus/event-consumer";
