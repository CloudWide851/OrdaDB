import "@fontsource-variable/manrope";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ConfigProvider } from "antd";
import zhCN from "antd/locale/zh_CN";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import App from "./App";
import "./styles.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ConfigProvider
      locale={zhCN}
      theme={{
        token: {
          colorPrimary: "#416E95",
          colorText: "#263946",
          colorBgBase: "#f7fafb",
          borderRadius: 8,
          fontFamily:
            '"Manrope Variable", "Microsoft YaHei UI", "PingFang SC", sans-serif',
          fontSize: 15,
        },
        components: {
          Tooltip: {
            fontSize: 14,
            colorBgSpotlight: "#263946",
            borderRadius: 7,
          },
        },
      }}
    >
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </ConfigProvider>
  </StrictMode>,
);
