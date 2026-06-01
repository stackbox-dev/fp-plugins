import FpEventBus from "./event-bus";
import FpFileStore from "./file-store";

export { FileStore } from "./file-store";
export { EventBus, EventBusOptions, EventMessage } from "./event-bus/interfaces";
export { EventConsumerBuilder } from "./event-bus/event-consumer/interface";

export const Plugins = {
  EventBus: FpEventBus,
  FileStore: FpFileStore,
};
