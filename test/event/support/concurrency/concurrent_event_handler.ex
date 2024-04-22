defmodule Commanded.Event.ConcurrentEventHandler do
  @moduledoc false
  alias Commanded.Event.ConcurrencyEvent

  use Commanded.Event.Handler,
    application: Commanded.DefaultApp,
    name: __MODULE__,
    concurrency: 5

  @impl Commanded.Event.Handler
  def init do
    Process.send(:test, {:init, self()}, [])
  end

  @impl Commanded.Event.Handler
  def init(config) do
    Process.send(:test, {:init, config, self()}, [])

    {:ok, config}
  end

  @impl Commanded.Event.Handler
  def handle(%ConcurrencyEvent{} = event, metadata) do
    %ConcurrencyEvent{stream_uuid: stream_uuid, index: index} = event
    index_to_fail_on = get_in(metadata, [:state, :fail_on])

    case index_to_fail_on do
      ^index ->
        Process.send(:test, {:handler_error, stream_uuid, self()}, [])
        {:error, :handler_error}

      _ ->
        Process.send(:test, {:event, stream_uuid, self()}, [])
        :ok
    end
  end

  @impl Commanded.Event.Handler
  def error(error, event, %{metadata: metadata}) do
    index_to_fail_on = get_in(metadata, [:state, :fail_on])

    if event.index == index_to_fail_on do
      {:stop, :normal}
    else
      {:stop, error}
    end
  end
end
