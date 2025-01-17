export default {
  fetch: async (...args) => {
    INSERT_INIT();

    const imports = require("./index_bg.js");

    // Run the worker's initialization function.
    imports.start?.();

    return imports.fetch(...args);
  },
  scheduled: async (...args) => {
    INSERT_INIT();
    
    const imports = require("./index_bg.js");

    // Run the worker's initialization function.
    imports.start?.();

    return imports.scheduled;
  },
  queue: async (...args) => {
    INSERT_INIT();
    
    const imports = require("./index_bg.js");

    // Run the worker's initialization function.
    imports.start?.();

    return imports.queue;
  },
};
