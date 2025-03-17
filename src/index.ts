import FpEventBus from "./event-bus";
export * from "./event-bus/interfaces";

const Plugins = {
  EventBus: FpEventBus,
};

export default Plugins;
