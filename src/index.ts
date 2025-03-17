import FpEventBus from "./event-bus";
export * from "./event-bus/interfaces";

export const Plugins = {
  EventBus: FpEventBus,
};

export * from "./event-bus/event-consumer";
